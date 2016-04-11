import { Meteor } from 'meteor/meteor';
import getUrls from 'get-urls';
import truncate from 'truncate-html';
import lodash from 'lodash';

Meteor.startup(() => {
  let remote = DDP.connect(Meteor.settings.remote);
  authenticate(remote);

  const Events = new Meteor.Collection('events', remote);
  const Categories = new Meteor.Collection('categories', remote);

  const Jobs = JobCollection('fundoQueue', {connection: remote});

  remote.subscribe('allJobs', function() {
    remote.subscribe('allEvents', function() {
      remote.subscribe('categories', function() {
        processJobs(Jobs, Events, Categories, remote);
      });
    });
  });

  remote.onReconnect = authenticate.bind(this, remote);
});


function authenticate(remote) {
  remote.call('login', {
    user: {email: Meteor.settings.admin.email},
    password: {digest: SHA256(Meteor.settings.admin.password), algorithm: 'sha-256'}
  }, function(err, res) {
    if (err) {
      console.log(err);
    }
  });
}

function processJobs(jobs, events, categories, remote) {
  processRefresh(jobs, events, categories, remote);
  processEventFetching(jobs, events, remote);
}

function processRefresh(jobs, events, categories, remote) {
  return jobs.processJobs('refresh',
    function(job, cb) {
      // Remove all events that have expired.
      console.log('REFRESHING...');
      let expiredEvents = events.find({expires: {$lt: new Date()}});
      //let expiredEvents = events.find();
      expiredEvents.forEach(function(event, index) {
        console.log('removing ' + event._id);
        events.remove(event._id);
      });

      //// Update all categories from eventful.
      Meteor.http.get("http://api.eventful.com/json/categories/list",
        {
          timeout: 30000,
          params: {
            app_key: Meteor.settings.eventfulAPIKey,
            subcategories: 1
          }
        },
        function(error, result) {
          if (error || result.statusCode != 200 || !result.data) {
            console.log('could not update categories from eventful');
            if (error)
              console.log(error);
            return;
          }

          let data = result.data;
          _.each(data.category, function(category) {
            remote.call('addCategory', category);
          });
        });


      //// Remove all categories that have expired.
      let expiredCategories = categories.find({expires: {$lt: new Date()}});
      expiredCategories.forEach(function(category, index) {
        category.remove(category._id);
      });

      job.done();
      cb();
    }
  );
}


function processEventFetching(jobs, events, remote) {
  return jobs.processJobs('fetchCity',
    function(job, cb) {
      let data = job.data;
      if (!data.city)
        return;
      data.page = data.page || 0;
      console.log('got job to fetch ' + data.city + ' page: ' + data.page);
      try {
        fetchPage(data.city, data.page, job, jobs, function(event) {
          remote.call('addEvent', event, events.findOne({_id: event.id}), data.city);
        }, function() {
          job.done();
          cb();
        });
      } catch (e) {
        job.fail(e);
      }
    }
  );
}


function fetchPage(city, page, thisJob, jobs, eventCallback, doneCallback) {
  const page_size = 50;
  const days = 30;
  const MAX_PAGES_TO_FETCH = Meteor.settings.maxPagesPerCity || 50;

  let done = false;
  let today = new Date();
  let endDate = new Date();
  endDate.setDate(today.getDate() + days);
  let date = formatEventfulDate(today) + "-" + formatEventfulDate(endDate);


  Meteor.http.get("http://api.eventful.com/json/events/search",
    {
      timeout: 30000,
      params: {
        app_key: Meteor.settings.eventfulAPIKey,
        page_size: page_size,
        date: date,
        where: city,
        within: '20',
        units: 'miles',
        sort_order: 'popularity',
        page_number: page,
        include: "price,categories,tickets,popularity,subcategories,mature",
        image_sizes: "medium,block,large,edpborder250,dropshadow250,dropshadow170,block178,thumb,small",
        mature: "normal",
        languages: "1"
      }
    },
    function(error, result) {
      if (error || result.statusCode != 200) {
        thisJob.fail(error || JSON.parse(result.content));
        return;
      }
      let resultJSON = JSON.parse(result.content);

      let events = resultJSON.events.event;

      console.log('got ' + events.length + ' events for page ' + page + ' in city ' + city);

      // If this isn't the last page, then add another job to fetch the next page.
      if (Math.min(resultJSON.page_count, MAX_PAGES_TO_FETCH) > page + 1) {
        var newJob = new Job(jobs, 'fetchCity', {
          page: page + 1,
          city: city
        });

        // Set some properties of the job and then submit it
        newJob.priority('normal')
          .retry({
            retries: 5,
            wait: 15 * 60 * 1000
          })  // 15 minutes between attempts
          .save();               // Commit it to the server
      } else {
        done = true;
      }

      _.each(events, function(event, index) {

        // If this event is not in english or undetermined OR if it doesn't have a start time, then don't save it.
        // This is done because undetermined events can still be high quality.
        if (!event.language ||
          (event.language.toLowerCase() != 'english' && event.language.toLowerCase() != 'undetermined') || !event.start_time) {
          return;
        }

        //Parse out all html tags from the description, and convert it to normal text.
        let description = truncate(event.description || "", {
          length: 1000,
          stripTags: false,
          ellipsis: '...',
          excludes: ['img', 'br'],
          decodeEntities: true
        });

        description = !description || description == 'null' || description.length == 0 ?
          null : description;


        // Extract any links from the description.
        event.links = event.description ? getUrls(event.description) : [];

        // Format the event category.
        _.map(event.categories.category, function(category) {
          category.name = truncate(category.name, {
            length: 100,
            stripTags: true,
            ellipsis: '...',
            excludes: ['img', 'br'],
            decodeEntities: true
          });
          return category;
        });

        event.start_time = event.start_time ? new Date(event.start_time) : null;
        event.stop_time = event.stop_time ? new Date(event.stop_time) : null;

        event.description = description;
        eventCallback(event);
      });

      if (done) {
        console.log('done!');
      }

      doneCallback();
    });
}

function formatEventfulDate(date) {
  var string = "";
  string += date.getFullYear();
  if (date.getMonth() < 9) {
    string += "0";
  }
  string += (date.getMonth() + 1);
  if (date.getDate() < 10) {
    string += "0";
  }
  string += date.getDate();
  string += "00";

  return string;
}
