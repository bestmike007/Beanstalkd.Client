using System.Collections.Generic;

namespace Beanstalkd.Client
{
    public class TubeStats
    {
        /// <summary>
        /// The tube's name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// the number of ready jobs with priority less than 1024 in this tube.
        /// </summary>
        public uint CurrentJobsUrgent { get; set; }

        /// <summary>
        /// the number of jobs in the ready queue in this tube.
        /// </summary>
        public uint CurrentJobsReady { get; set; }

        /// <summary>
        /// the number of jobs reserved by all clients in this tube.
        /// </summary>
        public uint CurrentJobsReserved { get; set; }

        /// <summary>
        /// the number of delayed jobs in this tube.
        /// </summary>
        public uint CurrentJobsDelayed { get; set; }

        /// <summary>
        /// the number of buried jobs in this tube.
        /// </summary>
        public uint CurrentJobsBuried { get; set; }

        /// <summary>
        /// the cumulative count of jobs created in this tube.
        /// </summary>
        public uint TotalJobs { get; set; }

        /// <summary>
        /// the number of open connections that have issued a reserve command while watching this tube but not yet received a response.
        /// </summary>
        public uint CurrentWaiting { get; set; }

        /// <summary>
        /// the number of open connections that are currently using this tube.
        /// </summary>
        public uint CurrentUsing { get; set; }

        /// <summary>
        /// the number of open connections that have issued a reserve command while watching this tube but not yet received a response.
        /// </summary>
        public uint CurrentWatching { get; set; }

        /// <summary>
        /// the number of seconds the tube has been paused for.
        /// </summary>
        public uint Pause { get; set; }

        /// <summary>
        /// the cumulative number of delete commands for this tube.
        /// </summary>
        public uint CmdDelete { get; set; }

        /// <summary>
        /// the cumulative number of pause-tube commands for this tube.
        /// </summary>
        public uint CmdPauseTube { get; set; }

        /// <summary>
        /// the number of seconds until the tube is un-paused.
        /// </summary>
        public uint PauseTimeLeft { get; set; }

        public static TubeStats Parse(Dictionary<string, string> data)
        {
            return new TubeStats
            {
                Name = data["name"],
                CurrentJobsUrgent = Parse(data, "current-jobs-urgent"),
                CurrentJobsReady = Parse(data, "current-jobs-ready"),
                CurrentJobsReserved = Parse(data, "current-jobs-reserved"),
                CurrentJobsDelayed = Parse(data, "current-jobs-delayed"),
                CurrentJobsBuried = Parse(data, "current-jobs-buried"),
                TotalJobs = Parse(data, "total-jobs"),
                CurrentWaiting = Parse(data, "current-waiting"),
                CurrentUsing = Parse(data, "current-using"),
                CurrentWatching = Parse(data, "current-watching"),
                Pause = Parse(data, "pause"),
                CmdDelete = Parse(data, "cmd-delete"),
                CmdPauseTube = Parse(data, "cmd-pause-tube"),
                PauseTimeLeft = Parse(data, "pause-time-left")
            };
        }

        private static uint Parse(IDictionary<string, string> data, string key, uint defaultValue = 0)
        {
            return data.ContainsKey(key) ? uint.Parse(data[key]) : defaultValue;
        }
    }
}