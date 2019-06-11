using System;
using System.Diagnostics;
using System.Threading;
using System.Timers;
using Timer = System.Timers.Timer;

namespace ConfluentKafkaPOC.Common
{
    public class RpsCounter : IDisposable
    {
        private bool _started;
        private const int ReportIntervalMilliseconds = 1000;
        private Stopwatch _stopwatch;
        private long _lastElapsed;
        private Timer _timer;
        private int _messages;
        private int _lastMessages;

        public void AddMessage()
        {
            if (! _started)
                Start();

            Interlocked.Increment(ref _messages);
        }

        public void Dispose()
        {
            if (!_started)
                return;
            _stopwatch.Stop();
            _timer.Stop();
            _timer.Dispose();
            Console.WriteLine($"RPS Avg: {_messages * 1000.0 / _stopwatch.ElapsedMilliseconds:F2}");
        }

        // Average from start time
        private double GetAvgRps()
        {
            return _messages * 1000.0 / _stopwatch.ElapsedMilliseconds;
        }

        // For last period
        private double GetRps()
        {
            return (_messages - _lastMessages) * 1000.0 / (_stopwatch.ElapsedMilliseconds - _lastElapsed);
        }

        private void Start()
        {
            _started = true;
            _stopwatch = Stopwatch.StartNew();
            _timer = new Timer(ReportIntervalMilliseconds);
            _timer.Elapsed += TimerOnElapsed;
            _timer.Start();
        }

        private void TimerOnElapsed(object sender, ElapsedEventArgs e)
        {
            Console.WriteLine($"RPS: {GetRps():F2}; Avg: {GetAvgRps():F2}");
            _lastMessages = _messages;
            _lastElapsed = _stopwatch.ElapsedMilliseconds;
        }
    }
}
