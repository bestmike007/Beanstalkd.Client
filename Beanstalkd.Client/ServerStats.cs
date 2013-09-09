using System.Collections.Generic;

namespace Beanstalkd.Client
{
    public class ServerStats
    {
        /// <summary>
        /// the number of ready jobs with priority less than 1024.
        /// </summary>
        public uint CurrentJobsUrgent { get; set; }

        /// <summary>
        /// the number of jobs in the ready queue.
        /// </summary>
        public uint CurrentJobsReady { get; set; }

        /// <summary>
        /// the number of jobs reserved by all clients.
        /// </summary>
        public uint CurrentJobsReserved { get; set; }

        /// <summary>
        /// the number of delayed jobs.
        /// </summary>
        public uint CurrentJobsDelayed { get; set; }

        /// <summary>
        /// the number of buried jobs.
        /// </summary>
        public uint CurrentJobsBuried { get; set; }

        /// <summary>
        /// the cumulative number of put commands.
        /// </summary>
        public uint CmdPut { get; set; }

        /// <summary>
        /// the cumulative number of peek commands.
        /// </summary>
        public uint CmdPeek { get; set; }

        /// <summary>
        /// the cumulative number of peek-ready commands.
        /// </summary>
        public uint CmdPeekReady { get; set; }

        /// <summary>
        /// the cumulative number of peek-delayed commands.
        /// </summary>
        public uint CmdPeekDelayed { get; set; }

        /// <summary>
        /// the cumulative number of peek-buried commands.
        /// </summary>
        public uint CmdPeekBuried { get; set; }

        /// <summary>
        /// the cumulative number of reserve commands.
        /// </summary>
        public uint CmdReserve { get; set; }

        /// <summary>
        /// the cumulative number of use commands.
        /// </summary>
        public uint CmdUse { get; set; }

        /// <summary>
        /// the cumulative number of watch commands.
        /// </summary>
        public uint CmdWatch { get; set; }

        /// <summary>
        /// the cumulative number of ignore commands.
        /// </summary>
        public uint CmdIgnore { get; set; }

        /// <summary>
        /// the cumulative number of delete commands.
        /// </summary>
        public uint CmdDelete { get; set; }

        /// <summary>
        /// the cumulative number of release commands.
        /// </summary>
        public uint CmdRelease { get; set; }

        /// <summary>
        /// the cumulative number of bury commands.
        /// </summary>
        public uint CmdBury { get; set; }

        /// <summary>
        /// the cumulative number of kick commands.
        /// </summary>
        public uint CmdKick { get; set; }

        /// <summary>
        /// the cumulative number of stats commands.
        /// </summary>
        public uint CmdStats { get; set; }

        /// <summary>
        /// the cumulative number of stats-job commands.
        /// </summary>
        public uint CmdStatsJob { get; set; }

        /// <summary>
        /// the cumulative number of stats-tube commands.
        /// </summary>
        public uint CmdStatsTube { get; set; }

        /// <summary>
        /// the cumulative number of list-tubes commands.
        /// </summary>
        public uint CmdListTubes { get; set; }

        /// <summary>
        /// the cumulative number of list-tube-used commands.
        /// </summary>
        public uint CmdCurrentTube { get; set; }

        /// <summary>
        /// the cumulative number of list-tubes-watched commands.
        /// </summary>
        public uint CmdListWatchingTubes { get; set; }

        /// <summary>
        /// the cumulative number of pause-tube commands.
        /// </summary>
        public uint CmdPauseTube { get; set; }

        /// <summary>
        /// the cumulative count of times a job has timed out.
        /// </summary>
        public uint TimeoutCount { get; set; }

        /// <summary>
        /// the cumulative count of jobs created.
        /// </summary>
        public uint TotalJobs { get; set; }

        /// <summary>
        /// the maximum number of bytes in a job.
        /// </summary>
        public uint MaxJobSize { get; set; }

        /// <summary>
        /// the number of currently-existing tubes.
        /// </summary>
        public uint TubeCount { get; set; }

        /// <summary>
        /// the number of currently open connections.
        /// </summary>
        public uint ConnectionCount { get; set; }

        /// <summary>
        /// the number of open connections that have each issued at least one put command.
        /// </summary>
        public uint ProducerCount { get; set; }

        /// <summary>
        /// the number of open connections that have each issued at least one reserve command.
        /// </summary>
        public uint WorkerCount { get; set; }

        /// <summary>
        /// the number of open connections that have issued a reserve command but not yet received a response.
        /// </summary>
        public uint WaitingCount { get; set; }

        /// <summary>
        /// the cumulative count of connections.
        /// </summary>
        public uint CumulativeConnectionCount { get; set; }

        /// <summary>
        /// the process id of the server.
        /// </summary>
        public uint ProcessId { get; set; }

        /// <summary>
        /// the version string of the server.
        /// </summary>
        public string Version { get; set; }

        /// <summary>
        /// the accumulated user CPU time of this process in seconds and microseconds.
        /// </summary>
        public uint UserTime { get; set; }

        /// <summary>
        /// the accumulated system CPU time of this process in seconds and microseconds.
        /// </summary>
        public uint SystemTime { get; set; }

        /// <summary>
        /// the number of seconds since this server started running.
        /// </summary>
        public uint UpTime { get; set; }

        /// <summary>
        /// the index of the oldest binlog file needed to store the current jobs
        /// </summary>
        public uint OldestBinlogIndex { get; set; }

        /// <summary>
        /// the index of the current binlog file being written to. If binlog is not active this value will be 0
        /// </summary>
        public uint CurrentBinlogIndex { get; set; }

        /// <summary>
        /// the maximum size in bytes a binlog file is allowed to get before a new binlog file is opened
        /// </summary>
        public uint MaxBinlogSize { get; set; }

        /// <summary>
        /// the cumulative number of records written to the binlog.
        /// </summary>
        public uint BinlogWrittenCount { get; set; }

        /// <summary>
        /// the cumulative number of records written as part of compaction.
        /// </summary>
        public uint BiglogMigratedCount { get; set; }

        /// <summary>
        /// a random id string for this server process, generated when each beanstalkd process starts.
        /// </summary>
        public string ProcessName { get; set; }

        /// <summary>
        /// the hostname of the machine as determined by uname.
        /// </summary>
        public string Hostname { get; set; }

        public static ServerStats Parse(Dictionary<string, string> data)
        {
            return new ServerStats
            {
                CurrentJobsUrgent = Parse(data, "current-jobs-urgent"),
                CurrentJobsReady = Parse(data, "current-jobs-ready"),
                CurrentJobsReserved = Parse(data, "current-jobs-reserved"),
                CurrentJobsDelayed = Parse(data, "current-jobs-delayed"),
                CurrentJobsBuried = Parse(data, "current-jobs-buried"),
                CmdPut = Parse(data, "cmd-put"),
                CmdPeek = Parse(data, "cmd-peek"),
                CmdPeekReady = Parse(data, "cmd-peek-ready"),
                CmdPeekDelayed = Parse(data, "cmd-peek-delayed"),
                CmdPeekBuried = Parse(data, "cmd-peek-buried"),
                CmdReserve = Parse(data, "cmd-reserve"),
                CmdUse = Parse(data, "cmd-use"),
                CmdWatch = Parse(data, "cmd-watch"),
                CmdIgnore = Parse(data, "cmd-ignore"),
                CmdDelete = Parse(data, "cmd-delete"),
                CmdRelease = Parse(data, "cmd-release"),
                CmdBury = Parse(data, "cmd-bury"),
                CmdKick = Parse(data, "cmd-kick"),
                CmdStats = Parse(data, "cmd-stats"),
                CmdStatsJob = Parse(data, "cmd-stats-job"),
                CmdStatsTube = Parse(data, "cmd-stats-tube"),
                CmdListTubes = Parse(data, "cmd-list-tubes"),
                CmdCurrentTube = Parse(data, "cmd-list-tube-used"),
                CmdListWatchingTubes = Parse(data, "cmd-list-tubes-watched"),
                CmdPauseTube = Parse(data, "cmd-pause-tube"),
                TimeoutCount = Parse(data, "job-timeouts"),
                TotalJobs = Parse(data, "total-jobs"),
                MaxJobSize = Parse(data, "max-job-size"),
                TubeCount = Parse(data, "current-tubes"),
                ConnectionCount = Parse(data, "current-connections"),
                ProducerCount = Parse(data, "current-producers"),
                WorkerCount = Parse(data, "current-workers"),
                WaitingCount = Parse(data, "current-waiting"),
                CumulativeConnectionCount = Parse(data, "total-connections"),
                ProcessId = Parse(data, "pid"),
                Version = data.ContainsKey("version") ? data["version"] : string.Empty,
                UserTime = (uint) (double.Parse(data["rusage-utime"])*1000000),
                SystemTime = (uint) (double.Parse(data["rusage-stime"])*1000000),
                UpTime = Parse(data, "uptime"),
                OldestBinlogIndex = Parse(data, "binlog-oldest-index"),
                CurrentBinlogIndex = Parse(data, "binlog-current-index"),
                MaxBinlogSize = Parse(data, "binlog-max-size"),
                BinlogWrittenCount = Parse(data, "binlog-records-written"),
                BiglogMigratedCount = Parse(data, "binlog-records-migrated"),
                ProcessName = data.ContainsKey("id") ? data["id"] : string.Empty,
                Hostname = data.ContainsKey("hostname") ? data["hostname"] : string.Empty
            };
        }

        private static uint Parse(IDictionary<string, string> data, string key, uint defaultValue = 0)
        {
            return data.ContainsKey(key) ? uint.Parse(data[key]) : defaultValue;
        }
    }
}