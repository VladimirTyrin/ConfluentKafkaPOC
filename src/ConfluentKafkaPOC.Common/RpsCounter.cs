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
        private long _messagesSize;
        private long _lastMessagesSize;

        public void AddMessage(int size)
        {
            if (! _started)
                Start();

            Interlocked.Increment(ref _messages);
            Interlocked.Add(ref _messagesSize, size);
        }

        public void Dispose()
        {
            if (!_started)
                return;
            _stopwatch.Stop();
            _timer.Stop();
            _timer.Dispose();
            Console.WriteLine($"RPS Avg: {GetAvgRps():F2}; Msg size avg: {GetAvgMessageSize():F1} bytes");
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

        private double GetAvgMessageSize()
        {
            return _messagesSize * 1.0 / _messages;
        }

        private double GetMessageSize()
        {
            return (_messagesSize - _lastMessagesSize) * 1.0 / (_messages - _lastMessages);
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
            Console.WriteLine($"RPS: {GetRps():F2}; Avg: {GetAvgRps():F2}; Msg size: {GetMessageSize():F1}; Avg: {GetAvgMessageSize():F1}");
            _lastMessages = _messages;
            _lastElapsed = _stopwatch.ElapsedMilliseconds;
            _lastMessagesSize = _messagesSize;
        }
    }
}
