using System;
using System.Collections.Generic;

namespace Beanstalkd.Client
{
    public interface IBeanstalkdClient : IDisposable
    {
        /// <summary>
        /// Get connection status.
        /// </summary>
        bool Disconnected { get; }

        /// <summary>
        /// True after the client is disposed
        /// </summary>
        bool Disposed { get; }

        #region Producer functions

        /// <summary>
        /// Get the current used tube name.
        /// </summary>
        string CurrentTube { get; }

        /// <summary>
        /// Set the tube to put jobs.
        /// </summary>
        /// <param name="tube">The tube name to set, 200 bytes at most</param>
        /// <returns>The tube name currently using</returns>
        string Use(string tube);

        /// <summary>
        /// Put job into currently using tube.
        /// </summary>
        /// <param name="data">The job to put</param>
        /// <param name="priority">Jobs with smaller priority values will be scheduled before jobs with larger priorities. The most urgent priority is 0.</param>
        /// <param name="delay">Number of seconds to wait before putting the job in the ready queue.</param>
        /// <param name="ttr">Time to run the job in seconds. The job will time out and the server will release the job.</param>
        /// <returns>Job id</returns>
        uint Put(byte[] data, uint priority = 4294967295, uint delay = 0, uint ttr = 10);

        /// <summary>
        /// Put job into a specific tube
        /// </summary>
        /// <param name="tube">The tube name to put the job</param>
        /// <param name="data">The job to put</param>
        /// <param name="priority">Jobs with smaller priority values will be scheduled before jobs with larger priorities. The most urgent priority is 0.</param>
        /// <param name="delay">Number of seconds to wait before putting the job in the ready queue.</param>
        /// <param name="ttr">Time to run the job in seconds. The job will time out and the server will release the job.</param>
        /// <returns>Job id</returns>
        uint Put(string tube, byte[] data, uint priority = 4294967295, uint delay = 0, uint ttr = 10);

        #endregion

        #region Worker functions

        /// <summary>
        /// Get the current watching tubes.
        /// </summary>
        List<string> WatchList { get; }

        /// <summary>
        /// Wait for next ready job.
        /// </summary>
        /// <param name="job">The job that is successfully reserved or null otherwise</param>
        /// <returns>The job reserved</returns>
        ReserveStatus Reserve(out Job job);

        /// <summary>
        /// Reserve a job with timeout.
        /// </summary>
        /// <param name="timeout">Number of seconds to wait before timeout. Return immediately if it's set to 0.</param>
        /// <param name="job">The job that is successfully reserved or null otherwise</param>
        /// <returns>The job reserve status</returns>
        ReserveStatus Reserve(uint timeout, out Job job);

        /// <summary>
        /// Delete a reserved job or a job in buried status.
        /// </summary>
        /// <param name="jobId">The id for the job.</param>
        /// <returns>true if the job is successfully removed</returns>
        bool Delete(uint jobId);

        /// <summary>
        /// Put a reserved job back into the tube.
        /// </summary>
        /// <param name="jobId">The id of the job</param>
        /// <param name="priority">Jobs with smaller priority values will be scheduled before jobs with larger priorities. The most urgent priority is 0.</param>
        /// <param name="delay">Number of seconds to wait before putting the job in the ready queue.</param>
        /// <returns>false if the job is not reserved by the current client</returns>
        bool Release(uint jobId, uint priority = 4294967295, uint delay = 0);

        /// <summary>
        /// Bury a reserved job.
        /// </summary>
        /// <param name="jobId">The id of the job</param>
        /// <param name="priority">Jobs with smaller priority values will be scheduled before jobs with larger priorities. The most urgent priority is 0.</param>
        /// <returns>false if the job is not reserved by the current client</returns>
        bool Bury(uint jobId, uint priority = 4294967295);


        /// <summary>
        /// Request more time to work on the job.
        /// </summary>
        /// <param name="jobId">The id of the job</param>
        /// <returns>false if the job is not reserved by the current client</returns>
        bool Touch(uint jobId);

        /// <summary>
        /// Add a tube to watch
        /// </summary>
        /// <param name="tube">Tube name to watch</param>
        /// <returns>The size of watch list</returns>
        uint Watch(string tube);

        /// <summary>
        /// Ignore a tube, that is to stop watching a tube
        /// </summary>
        /// <param name="tube">Tube name to ignore</param>
        /// <returns>false if the tube is not in the watching list</returns>
        bool Ignore(string tube);

        #endregion

        #region Other functions

        /// <summary>
        /// Peek a job.
        /// </summary>
        /// <param name="jobId">The id of the job to peek</param>
        /// <returns>null if job not exists</returns>
        Job Peek(uint jobId);

        /// <summary>
        /// Peek next ready job.
        /// </summary>
        /// <returns>null if no job in ready state</returns>
        Job PeekReady();

        /// <summary>
        /// Peek next buried job
        /// </summary>
        /// <returns>null if no job in ready state</returns>
        Job PeekBuried();

        /// <summary>
        /// Peek next delayed job
        /// </summary>
        /// <returns>null if no job in delayed state</returns>
        Job PeekDelayed();

        /// <summary>
        /// Moves no more than a specific bound number of jobs from nonready state to ready state in current used tube. 
        /// If there are any buried jobs, it will only kick buried jobs. Otherwise it will kick delayed jobs.
        /// </summary>
        /// <param name="bound">The maximum number of jobs to kick</param>
        /// <returns>the number of jobs actually kicked</returns>
        uint Kick(uint bound);


        /// <summary>
        /// Get the stats of a specific job
        /// </summary>
        /// <param name="jobId">the job id</param>
        /// <returns>null if job not found</returns>
        JobStats StatsJob(uint jobId);

        /// <summary>
        /// Get the stats of a specific tube
        /// </summary>
        /// <param name="tube">the name of the tube</param>
        /// <returns>null if tube not found</returns>
        TubeStats StatsTube(string tube);

        /// <summary>
        /// Get the stats of the beanstalkd server
        /// </summary>
        /// <returns></returns>
        ServerStats Stats();

        /// <summary>
        /// Returns a list of all existing tubes.
        /// </summary>
        /// <returns></returns>
        List<string> ListTubes();

        #endregion
    }
}