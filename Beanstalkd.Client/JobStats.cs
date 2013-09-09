using System;
using System.Collections.Generic;
using System.Text;

namespace Beanstalkd.Client
{
    public class JobStats
    {
        /// <summary>
        /// the job id
        /// </summary>
        public uint JobId { get; set; }

        /// <summary>
        /// the name of the tube that contains this job
        /// </summary>
        public string Tube { get; set; }

        /// <summary>
        /// "ready" or "delayed" or "reserved" or "buried"
        /// </summary>
        public JobStatus Status { get; set; }

        /// <summary>
        /// the priority value set by the put, release, or bury commands.
        /// </summary>
        public uint Priority { get; set; }

        /// <summary>
        /// the time in seconds since the put command that created this job.
        /// </summary>
        public uint Age { get; set; }

        /// <summary>
        /// is the number of the earliest binlog file containing this job. If -b wasn't used, this will be 0.
        /// </summary>
        public uint BinlogIndex { get; set; }

        /// <summary>
        /// the number of seconds left until the server puts this job
        /// into the ready queue. This number is only meaningful if the job is
        /// reserved or delayed. If the job is reserved and this amount of time
        /// elapses before its state changes, it is considered to have timed out.
        /// </summary>
        public uint TimeLeft { get; set; }

        /// <summary>
        /// the number of times this job has been reserved.
        /// </summary>
        public uint ReserveCount { get; set; }

        /// <summary>
        /// the number of times this job has timed out during a reservation.
        /// </summary>
        public uint TimeoutCount { get; set; }

        /// <summary>
        /// the number of times a client has released this job from a reservation.
        /// </summary>
        public uint ReleaseCount { get; set; }

        /// <summary>
        /// the number of times this job has been buried.
        /// </summary>
        public uint BuryCount { get; set; }

        /// <summary>
        /// the number of times this job has been kicked.
        /// </summary>
        public uint KickCount { get; set; }

        public static JobStats Parse(Dictionary<string, string> data)
        {
            var status = data["state"];
            status = char.ToUpper(status[0]) + status.Substring(1);
            return new JobStats
            {
                JobId = Parse(data, "id"),
                Tube = data["tube"],
                Status = (JobStatus) Enum.Parse(typeof (JobStatus), status),
                Priority = Parse(data, "pri"),
                Age = Parse(data, "age"),
                TimeLeft = Parse(data, "time-left"),
                ReserveCount = Parse(data, "reserves"),
                TimeoutCount = Parse(data, "timeouts"),
                ReleaseCount = Parse(data, "releases"),
                BuryCount = Parse(data, "buries"),
                KickCount = Parse(data, "kicks"),
                BinlogIndex = Parse(data, "file")
            };
        }

        private static uint Parse(IDictionary<string, string> data, string key, uint defaultValue = 0)
        {
            return data.ContainsKey(key) ? uint.Parse(data[key]) : defaultValue;
        }
    }
}