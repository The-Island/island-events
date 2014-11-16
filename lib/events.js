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

// Subscribe user to an entity.
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
  self.db.Subscriptions.create(props, {inflate: {subscriber: profiles.user,
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

// Unsubscribe user to an entity.
Events.prototype.unsubscribe = function (subscriber, subscribee, cb) {
  if (!cb) cb = function(){};
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
        self.publish('usr-' + sub.subscriber_id.toString(),
            sub.meta.style + '.removed', {data: {id: sub._id.toString()}});
        if (sub.meta.style === 'follow') {
          self.publish('usr-' + sub.subscribee_id.toString(),
              sub.meta.style + '.removed', {data: {id: sub._id.toString()}});
        }
        _.each(notes, function (note) {
          self.publish('usr-' + note.subscriber_id.toString(),
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

// Accept user follow request.
Events.prototype.accept = function (subscription, cb) {
  if (!cb) cb = function(){};
  var self = this;

  Step(
    function () {

      // Get subscription users.
      self.db.inflate(subscription, {subscriber: profiles.user,
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
  cb = cb || function(){};
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
      self.send('usr-' + author_id.toString(), topic, data);
    }
  }

  // Create the event.
  if (!params.event) {
    return cb();
  }
  params.event.date = params.data.date || params.data.created;
  self.db.Events.create(params.event, function (err, event) {
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
          subscriber: _.extend(_.clone(profiles.user), {
            primaryEmail: 1,
            config: 1
          })
        };
        if (params.notify && params.notify.subscribee) {
          inflate.subscribee = _.extend(_.clone(profiles.user), {
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
              self.send('usr-' + event.actor_id.toString(), 'event.new', data);
            }

            // Publish event to subscribers.
            if (self.sock && event.public !== false) {
              _.each(subs, function (sub) {
                self.send('usr-' + sub.subscriber._id.toString(), 'event.new', data);
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
                    self.send('usr-' + recipient._id.toString(), 'notification.new',
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
  });
}

// Inflate and fill event properties.
Events.prototype.inflate = function(e, query, cb) {
  var self = this;

  function __dataset(o, p, cb) {
    Step(
      function () {
        self.db.inflate(o[p], {author: profiles.user}, this.parallel());
        self.db.fill(o[p], 'Channels', 'parent_id', {sort: {created: -1},
            limit: 5}, this.parallel());
        self.db.fill(o[p], 'Comments', 'parent_id', {sort: {created: -1},
            limit: 5, inflate: {author: profiles.user}},
            this.parallel());
        self.db.fill(o[p], 'Notes', 'parent_id', {sort: {created: -1},
            limit: 5, inflate: {author: profiles.user}},
            this.parallel());
      }, cb
    );
  }

  function __view(o, p, cb) {
    Step(
      function () {

        // Check access.
        var allow = true;
        if (query === false) {
          return this(null, allow);
        }
        var _this = _.after(_.size(o[p].datasets), _.bind(function (err) {
          this(err, allow);
        }, this));
        _.each(o[p].datasets, function (meta, did) {
          self.db.Datasets.read({_id: Number(did)}, function (err, doc) {
            if (err) return _this(err);
            if (!doc) return _this();
            var requestor = query.user_id ? {_id: query.user_id}: undefined;
            hasAccess(self.db, requestor, doc, function (err, _allow) {
              if (err) return _this(err);
              if (!_allow) {
                allow = false;
              }
              _this();
            });
          });
        });
      },
      function (err, allow) {
        if (err) return this(err);
        if (!allow) {
          e._reject = true;
          return this();
        }
        self.db.inflate(o[p], {author: profiles.user}, this.parallel());
        self.db.fill(o[p], 'Comments', 'parent_id', {sort: {created: -1},
            limit: 5, inflate: {author: profiles.user}}, this.parallel());
        self.db.fill(o[p], 'Notes', 'parent_id', {sort: {created: -1},
            limit: 5, inflate: {author: profiles.user}}, this.parallel());
      },
      cb
    );
  }

  function __note(o, p, cb) {
    Step(
      function () {
        self.db.inflate(o[p], {author: profiles.user}, this.parallel());
        self.db.fill(o[p], 'Comments', 'parent_id', {sort: {created: -1},
            limit: 5, inflate: {author: profiles.user}},
            this.parallel());
        self.db.inflate(o, {target: {collection: o[p].parent_type, '*': 1}},
            this.parallel());
      },
      function (err) {
        if (err) return this(err);
        switch (o[p].parent_type) {
          case 'dataset':
            __dataset(o, 'target', this);
            break;
          case 'view':
            __view(o, 'target', this);
            break;
        }
      },
      cb
    );
  }

  function __comment(o, cb) {
    var parent_type = o.action.parent_type || 'note';
    Step(
      function () {
        self.db.inflate(o.action, {author: profiles.user}, this.parallel());
        self.db.inflate(o, {target: {collection: parent_type, '*': 1}},
            this.parallel());
      },
      function (err) {
        if (err) return this(err);
        switch (parent_type) {
          case 'dataset':
            __dataset(o, 'target', this);
            break;
          case 'view':
            __view(o, 'target', this);
            break;
          case 'note':
            Step(
              function () {
                self.db.inflate(o.target, {author: profiles.user,
                    parent: {collection: o.target.parent_type, '*': 1}}, this.parallel());
                self.db.fill(o.target, 'Comments', 'parent_id', {sort: {created: -1},
                    limit: 5, inflate: {author: profiles.user}},
                    this.parallel());
              },
              function (err) {
                if (err) return this(err);
                switch (o.target.parent_type) {
                  case 'dataset':
                    __dataset(o.target, 'parent', this);
                    break;
                  case 'view':
                    __view(o.target, 'parent', this);
                    break;
                }
              },
              this
            );
            break;
        }
      },
      cb
    );
  }

  Step(
    function () {
      switch (e.action_type) {
        case 'dataset':
          __dataset(e, 'action', this);
          break;
        case 'view':
          __view(e, 'action', this);
          break;
        case 'note':
          __note(e, 'action', this);
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
