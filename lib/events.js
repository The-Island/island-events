/*
 * events.js: Handling for Island Events.
 *
 */

// Module Dependencies
var Step = require('step');
var util = require('util');
var sutil = require('island-util');
var _ = require('underscore');
_.mixin(require('underscore.string'));
_.mixin(require('underscore.deep'));
var collections = require('island-collections');
var profiles = collections.profiles;
var hasAccess = collections.hasAccess;

// Constuctor
var Events = exports.Events = function (opts) {
  opts = opts || {};
  this.db = opts.db;
  this.sock = opts.sock;
  this.emailer = opts.emailer;
}

// Send data over socket.
Events.prototype.send = function (channel, topic, data) {
  this.sock.send([channel, topic, JSON.stringify(data)].join(' '));
}

// Subscribe member to an entity.
Events.prototype.subscribe = function (subscriber, subscribee, meta, cb) {
  if (!cb) cb = function(){};
  var self = this;
  if (typeof subscriber._id === 'string') {
    subscriber._id = self.db.oid(subscriber._id);
  }
  if (typeof subscribee._id === 'string') {
    subscribee._id = self.db.oid(subscribee._id);
  }
  var props = {
    subscriber_id: subscriber._id,
    subscribee_id: subscribee._id,
    meta: meta,
    mute: false
  };
  self.db.Subscriptions.create(props, {inflate: {subscriber: profiles.member,
      subscribee: profiles[meta.type]}}, function (err, subscription) {
    if (err && err.code !== 11000 || !subscription) {
      return cb(err);
    }

    if (meta.style === 'watch') {
      self.publish(meta.style, meta.style + '.new', {data: subscription});
    } else {
      self.publish(meta.style, meta.style + '.new', {
        data: subscription,
        event: {
          actor_id: subscription.subscriber._id,
          target_id: subscription.subscribee._id,
          action_id: subscription._id,
          action_type: subscription.meta.style,
          data: {
            action: {
              i: subscription.subscriber._id.toString(),
              a: subscription.subscriber.displayName,
              g: subscription.subscriber.gravatar,
              t: subscription.meta.style,
              s: subscription.subscriber.username
            },
            target: {
              i: subscription.subscribee._id.toString(),
              a: subscription.subscribee.displayName,
              s: subscription.subscribee.username
            }
          }
        },
        options: {
          method: 'WITH_SUBSCRIPTION',
          subscription_id: subscription._id
        },
        notify: {subscribee: true}
      });
    }

    cb(null, subscription);
  });
}

// Unsubscribe member to an entity.
Events.prototype.unsubscribe = function (subscriber, subscribee, cb) {
  if (!cb) {
    cb = function(){};
  }
  var self = this;
  var props = {
    subscriber_id: subscriber._id ? subscriber._id: subscriber,
    subscribee_id: subscribee._id ? subscribee._id: subscribee
  };
  self.db.Subscriptions.read(props, function (err, sub) {
    if (err || !sub) {
      return cb(err);
    }

    Step(
      function () {

        // Remove subscription and list it's notes.
        self.db.Subscriptions.remove({_id: sub._id}, this.parallel());
        self.db.Notifications.list({subscription_id: sub._id}, this.parallel());
      },
      function (err, stat, notes) {
        if (err || !stat) {
          return this(err);
        }

        // Publish removed statuses.
        self.publish('mem-' + sub.subscriber_id.toString(),
            sub.meta.style + '.removed', {data: {id: sub._id.toString()}});
        if (sub.meta.style === 'follow') {
          self.publish('mem-' + sub.subscribee_id.toString(),
              sub.meta.style + '.removed', {data: {id: sub._id.toString()}});
        }
        _.each(notes, function (note) {
          self.publish('mem-' + note.subscriber_id.toString(),
              'notification.removed', {data: {id: note._id.toString()}});
        });

        // Remove notes.
        self.db.Notifications.remove({subscription_id: sub._id}, this);
      },
      function (err, stat) {
        if (err) return cb(err);
        cb(null, sub);
      }
    );
  });
}

// Accept member follow request.
Events.prototype.accept = function (subscription, cb) {
  if (!cb) {
    cb = function(){};
  }
  var self = this;

  Step(
    function () {

      // Get subscription members.
      self.db.inflate(subscription, {subscriber: profiles.member,
          subscribee: profiles[subscription.meta.type]}, this.parallel());

      // Update subscription style.
      self.db.Subscriptions.update({_id: subscription._id},
          {$set: {'meta.style': 'follow'}}, this.parallel());
    },
    function (err, stat) {
      if (err || !stat) {
        return cb(err);
      }

      // Publish accept event.
      self.publish('accept', 'accept.new', {
        data: subscription,
        event: {
          actor_id: subscription.subscribee._id,
          target_id: subscription.subscriber._id,
          action_id: subscription._id,
          action_type: 'accept',
          data: {
            action: {
              i: subscription.subscribee._id.toString(),
              a: subscription.subscribee.displayName,
              g: subscription.subscribee.gravatar,
              t: 'accept',
              s: subscription.subscribee.username
            },
            target: {
              i: subscription.subscriber._id.toString(),
              a: subscription.subscriber.displayName,
              s: subscription.subscriber.username
            }
          }
        },
        options: {
          method: 'WITH_SUBSCRIPTION',
          subscription_id: subscription._id
        },
        notify: {subscriber: true}
      });

      // Publish follow event.
      self.publish('follow', 'follow.new', {
        data: subscription,
        event: {
          actor_id: subscription.subscriber._id,
          target_id: subscription.subscribee._id,
          action_id: subscription._id,
          action_type: 'follow',
          data: {
            action: {
              i: subscription.subscriber._id.toString(),
              a: subscription.subscriber.displayName,
              g: sutil.hash(subscription.subscriber.primaryEmail || 'foo@bar.baz'),
              t: 'follow',
              s: subscription.subscriber.username
            },
            target: {
              i: subscription.subscribee._id.toString(),
              a: subscription.subscribee.displayName,
              s: subscription.subscribee.username
            }
          }
        },
        options: {
          method: 'WITH_SUBSCRIPTION',
          subscription_id: subscription._id
        },
        notify: {subscribee: true}
      });

      cb(null, subscription);
    }
  );
}

// Publish data over a channel with a topic.
// Optionally create an event.
// Optionally create a notification.
Events.prototype.publish = function (channel, topic, params, cb) {
  if (typeof params === 'function') {
    cb = params;
    params = {};
  }
  params = _.deepClone(params || {});
  cb = {
    cb || function(){};
  }
  if (!channel || !topic || !params.data) {
    return cb('Invalid data');
  }
  var self = this;

  var options = _.defaults(params.options || {}, {
    method: 'DEMAND_SUBSCRIPTION'
  });

  // Publish raw data (for static lists, tickers).
  if (self.sock) {
    var data = sutil.client(_.deepClone(params.data));
    if (params.data.public !== false) {

      // Public event, send to all.
      self.send(channel, topic, data);
    } else {

      // Private event, send to author.
      var author_id = params.data.author ? params.data.author._id:
          (params.data.actor ? params.data.actor._id:
          params.data.author_id || params.data.actor_id);
      self.send('mem-' + author_id.toString(), topic, data);
    }
  }

  // Create the event.
  if (!params.event) {
    return cb();
  }
  Step(
    function () {
      if (params.event._id) {
        db.Events.update({_id: params.event._id}, {$set: params.event.$set},
            _.bind(function (err) {
          if (err) return this(err);
          db.Events.read({_id: params.event._id}, this);
        }, this));
      } else {
        params.event.date = params.data.date || params.data.created;
        db.Events.create(params.event, this);
      }
    },
    function (err, event) {
      if (err) return cb(err);

      Step(
        function () {
          var query;

          if (options.method === 'DEMAND_SUBSCRIPTION') {

            // Get 'follow' subscriptions.
            query = {
              subscribee_id: event.actor_id,
              'meta.style': 'follow',
              mute: false
            };

            // Get 'watch' subscriptions (if there's something to watch).
            if (event.target_id) {
              query = {$or: [query, {
                subscribee_id: event.target_id,
                'meta.style': 'watch',
                mute: false
              }]};
            }
          } else if (options.method === 'DEMAND_WATCH_SUBSCRIPTION') {

            // Get 'watch' subscriptions (if there's something to watch).
            if (event.target_id) {
              query = {
                subscribee_id: event.target_id,
                'meta.style': 'watch',
                mute: false
              };
            }
          } else if (options.method === 'DEMAND_WATCH_SUBSCRIPTION_FROM_AUTHOR') {

            // Get 'watch' subscriptions (if there's something to watch).
            if (event.target_id && event.target_author_id) {
              query = {
                subscriber_id: event.target_author_id,
                subscribee_id: event.target_id,
                'meta.style': 'watch',
                mute: false
              };
            }
          } else if (options.method === 'WITH_SUBSCRIPTION') {
            query = {_id: options.subscription_id};
          }

          // List subs.
          if (!query) {
            return this(null, []);
          }
          var inflate = {
            subscriber: _.extend(_.clone(profiles.member), {
              primaryEmail: 1,
              config: 1
            })
          };
          if (params.notify && params.notify.subscribee) {
            inflate.subscribee = _.extend(_.clone(profiles.member), {
              primaryEmail: 1,
              config: 1
            });
          }
          self.db.Subscriptions.list(query, {inflate: inflate}, this);
        },
        function (err, subs) {
          if (err) return this(err);
          if (subs.length === 0 || options.method !== 'DEMAND_WATCH_SUBSCRIPTION') {
            return this(null, subs);
          }

          // Ensure access is allowed.
          var _this = _.after(subs.length, _.bind(function (err) {
            if (err) return this(err);
            subs = _.reject(subs, function (s) {
              return s.reject;
            });
            this(null, subs);
          }, this));
          _.each(subs, function (s) {
            self.db.inflate(s, {subscribee: {collection: s.meta.type, '*': 1}},
                function (err) {
              if (err) return _this(err);
              hasAccess(self.db, s.subscriber, s.subscribee, function (err, allow) {
                if (err) return _this(err);
                if (!allow) {
                  s.reject = true;
                }
                _this();
              });
            });
          });
        },
        function (err, subs) {
          if (err) return cb(err);

          var data = _.deepClone(event);
          Step(
            function () {
              if (_.contains(['watch', 'follow', 'request', 'accept'],
                  data.action_type)) {
                return this();
              }

              // Inflate event action.
              self.db.inflate(data, {action: {collection: data.action_type,
                  '*': 1}}, this);
            },
            function (err) {
              if (err) return this(err);

              // Inflate event data for client.
              self.inflate(data, false, this);
            },
            function (err) {
              if (err) return cb(err);
              data = sutil.client(data);

              // Publish event to creator.
              if (self.sock) {
                self.send('mem-' + event.actor_id.toString(), 'event.new', data);
              }

              // Publish event to subscribers.
              if (self.sock && event.public !== false) {
                _.each(subs, function (sub) {
                  self.send('mem-' + sub.subscriber._id.toString(), 'event.new', data);
                });
              }

              // If notify, create a notification for each subscriber.
              if (subs.length === 0 || !params.notify
                  || _.isEmpty(params.notify)) {
                return cb();
              }

              var _cb = _.after(subs.length, cb);
              _.each(subs, function (sub) {
                var __cb = _.after(_.size(params.notify), _cb);
                _.each(params.notify, function (v, k) {
                  var recipient = sub[k];
                  if (recipient._id.toString() === sub.subscriber._id.toString()
                      && sub.subscriber._id.toString() === event.actor_id.toString()) {
                    return;
                  }

                  // Create the notification.
                  self.db.Notifications.create({
                    subscriber_id: recipient._id,
                    subscription_id: sub._id,
                    event_id: event._id,
                    read: false
                  }, function (err, note) {
                    if (err) return __cb(err);

                    // Publish notification.
                    note.event = event;
                    if (self.sock) {
                      self.send('mem-' + recipient._id.toString(), 'notification.new',
                          sutil.client(note));
                    }

                    // Handle notification delivery types by subscriber config.
                    var notifications = recipient.config.notifications;
                    var email = notifications[channel] && notifications[channel].email;
                    if (self.emailer && (email === true || email === 'true')
                        && recipient.primaryEmail !== undefined
                        && recipient.primaryEmail !== ''
                        && process.env.NODE_ENV === 'production'
                        ) {
                      self.emailer.notify(recipient, note, params.data.body, function (err) {
                        if (err) {
                          console.error(err);
                        }
                      });
                    }

                    // Don't wait for email task.
                    __cb();
                  });
                });
              });
            }
          );
        }
      );
    }
  );
}

// Inflate and fill event properties.
Events.prototype.inflate = function(e, query, cb) {
  var self = this;

  function __post(o, p, cb) {
    Step(
      function () {
        self.db.inflate(o[p], {author: profiles.member}, this.parallel());
        self.db.fill(o[p], 'Medias', 'parent_id', {sort: {created: -1}},
            this.parallel());
        self.db.fill(o[p], 'Comments', 'parent_id', {sort: {created: -1},
            limit: 5, reverse: true, inflate: {author: profiles.member}},
            this.parallel());
        self.db.fill(o[p], 'Hangtens', 'parent_id', this.parallel());
      }, cb
    );
  }

  function __session(o, p, cb) {
    Step(
      function () {
        self.db.inflate(o[p], {author: profiles.member, crag: profiles.crag},
            this.parallel());
        self.db.fill(o[p], 'Actions', 'session_id', {sort: {index: 1}},
            this.parallel());
      },
      function (err) {
        if (err) return this(err);
        self.db.fill(o[p].actions, 'Ticks', 'action_id', {sort: {index: 1}},
            this);
      },
      function (err) {
        if (err) return this(err);
        var tickCnt = 0;
        _.each(o[p].actions, function (a) {
          _.each(a.ticks, function (t) { ++tickCnt; });
        });
        if (tickCnt === 0) {
          return this();
        }
        var _this = _.after(tickCnt, this);
        _.each(o[p].actions, function (a) {
          _.each(a.ticks, function (t) {
            __tick(t, null, _this);
          });
        });
      },
      function (err) {
        if (err) return this(err);
        var tickCnt = 0;
        _.each(o[p].actions, function (a) {
          a.ticks = _.reject(a.ticks, function (t) {
            return t._reject;
          });
          tickCnt += a.ticks.length;
        });
        if (tickCnt === 0) {
          e._reject = true;
          return this();
        }
      },
      cb
    );
  }

  function __tick(o, p, cb) {
    var t = p ? o[p]: o;
    Step(
      function () {
        if (query === false) {
          return this();
        }
        var requestor = query.member_id ? {_id: query.member_id}: undefined;
        hasAccess(self.db, requestor, t, this);
      },
      function (err, _allow) {
        if (err) return _this(err);
        if (!_allow) {
          [p ? e: t]._reject = true;
          return this();
        }
        self.db.inflate(t, {author: profiles.member, ascent: profiles.ascent,
            crag: profiles.crag}, this.parallel());
        self.db.fill(t, 'Medias', 'parent_id', {sort: {created: -1}},
            this.parallel());
        self.db.fill(t, 'Comments', 'parent_id', {sort: {created: -1},
            limit: 5, reverse: true, inflate: {author: profiles.member}},
            this.parallel());
        self.db.fill(t, 'Hangtens', 'parent_id', this.parallel());
      }, cb
    );
  }

  function __crag(o, p, cb) {
    Step(
      function () {
        self.db.inflate(o[p], {author: profiles.member}, this.parallel());
        self.db.fill(o[p], 'Hangtens', 'parent_id', this.parallel());
      }, cb
    );
  }

  function __ascent(o, p, cb) {
    Step(
      function () {
        self.db.inflate(o[p], {author: profiles.member,
            crag: profiles.crag}, this.parallel());
        self.db.fill(o[p], 'Hangtens', 'parent_id', this.parallel());
      }, cb
    );
  }

  function __hangten(o, cb) {
    var parent_type = o.action.parent_type;
    Step(
      function () {
        self.db.inflate(o.action, {author: profiles.member}, this.parallel());
        self.db.inflate(o, {target: {collection: parent_type, '*': 1}},
            this.parallel());
      },
      function (err) {
        if (err) return this(err);
        switch (parent_type) {
          case 'post':
            __post(o, 'target', this);
            break;
          case 'tick':
            __tick(o, 'target', this);
            break;
        }
      },
      cb
    );
  }

  function __comment(o, cb) {
    var parent_type = o.action.parent_type;
    Step(
      function () {
        self.db.inflate(o.action, {author: profiles.member}, this.parallel());
        self.db.inflate(o, {target: {collection: parent_type, '*': 1}},
            this.parallel());
      },
      function (err) {
        if (err) return this(err);
        switch (parent_type) {
          case 'post':
            __post(o, 'target', this);
            break;
          case 'tick':
            __tick(o, 'target', this);
            break;
        }
      },
      cb
    );
  }

  Step(
    function () {
      switch (e.action_type) {
        case 'post':
          __post(e, 'action', this);
          break;
        case 'session':
          __session(e, 'action', this);
          break;
        case 'tick':
          __tick(e, 'action', this);
          break;
        case 'crag':
          __tick(e, 'action', this);
          break;
        case 'ascent':
          __tick(e, 'action', this);
          break;
        case 'hangten':
          __hangten(e, this);
          break;
        case 'comment':
          __comment(e, this);
          break;
        default: cb(); break;
      }
    },
    function (err) {
      cb(err);
    }
  );
}
