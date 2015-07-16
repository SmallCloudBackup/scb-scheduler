/**
 * SmallCloudBackup - Scheduler module
 *
 * A small node.js + rabbitmq task queue tool.
 *
 * @author Valentin Alexeev <valentin.alekseev@gmail.com>
 */

function Scheduler() {
  var jackrabbit = require("jackrabbit");
  var async = require("async");

  /**
   * Enqueue a new job with specific jobId
   * @param jobId String job id defined in the application configuration
   */
  this.enqueue = function (jobId) {
    console.log("Queuing new backup job: " + jobId);
    var queue;
    async.waterfall([
      function connectRabbitMQ (next) {
        queue = jackrabbit(process.env.CLOUDAMQP_URL)
          .on('connected', function() {
            console.log("Connected to queue");
            next();
          })
          .on('error', function(err) {
            console.log({ type: 'error', msg: err, service: 'rabbitmq' });
          })
          .on('disconnected', function() {
            console.log("Disconnected from queue.");
          });
      },
      function touchQueue (next) {
        queue.create("main", { durable: true }, next);
      },
      function sendJob (queue_instance, queue_info, next) {
        queue.publish("main", { jobId: jobId }, next());
      },
      function disconnect(next) {
        console.log("Message sent to queue.");
        queue.close();
        next();
      }
    ], allDone);
  }

  function allDone() {
    console.log("Queue closed, all done.");
  }
}

module.exports = new Scheduler();
