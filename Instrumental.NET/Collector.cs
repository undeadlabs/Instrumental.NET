//  Copyright 2014 Bloomerang
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

using System;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Net.Sockets;
using System.Threading;
using NLog;

namespace Instrumental.NET {
    class Collector {
        private const int ApproximateMaxMessages = 2000;
        private const int Backoff = 2;
        private const int MaxReconnectDelay = 15;

        private readonly string _apiKey;
        private readonly AutoResetEvent _event = new AutoResetEvent(false);
        private readonly ConcurrentQueue<String> _messages = new ConcurrentQueue<String>();
        private String _currentCommand;
        private Thread _worker;
        private int _queueFullWarned;
        private static readonly Logger _log = LogManager.GetCurrentClassLogger();

        public Collector (String apiKey) {
            _apiKey = apiKey;
            _worker = new Thread(WorkerLoop) { IsBackground = true };
            _worker.Start();
        }

        public void SendMessage (String message) {
            // Make sure the message is terminated with "\n" and includes no other "\r\n" characters
#if DEBUG
            if (message.IndexOf("\r") != -1 || message.IndexOf("\n") != message.Length - 1)
                throw new InstrumentalException("Invalid message, {0}", message);
#endif

            if (_messages.Count <= ApproximateMaxMessages) {
                _messages.Enqueue(message);
                _event.Set();
                if (_queueFullWarned != 0 && _messages.Count < ApproximateMaxMessages / 2) {
                    if (0 != Interlocked.Exchange(ref _queueFullWarned, 0))
                        _log.Info("Queue available again {0}", _messages.Count);
                }
            }
            else {
                if (0 == Interlocked.Exchange(ref _queueFullWarned, 1))
                    _log.Warn("Queue full; dropping messages until there's room");
            }
        }

        private void WorkerLoop () {
            while (true) {
                Socket socket = null;
                var failures = 0;

                try {
                    socket = Connect();
                    Authenticate(socket);
                    failures = 0;
                    SendQueuedMessages(socket);
                }
                catch (Exception e) {
                    if (socket != null) {
                        try {
                            socket.Disconnect(false);
                        }
                        catch { }
                        socket = null;
                    }

                    var delay = (int)Math.Min(MaxReconnectDelay, Math.Pow(failures++, Backoff));

                    // Only log at the ERROR level once so the logs aren't filled with warnings
                    // when the instrumental service goes down
                    LogLevel level;
                    if (failures < 3)
                        level = LogLevel.Debug;
                    else if (failures == 3)
                        level = LogLevel.Error;
                    else
                        level = LogLevel.Warn;
                    _log.Log(level, "{0} [{1} failures in a row] [Count {2}]", e.Message, failures, _messages.Count);

                    Thread.Sleep(delay * 1000);
                }
            }
        }

        private void SendQueuedMessages (Socket socket) {
            while (true) {
                if (_currentCommand == null) {
                    if (!_messages.TryDequeue(out _currentCommand)) {
                        _event.WaitOne();
                        continue;
                    }
                }

                if (IsSocketDisconnected(socket))
                    throw new Exception("Disconnected");

                var data = System.Text.Encoding.ASCII.GetBytes(_currentCommand);
                socket.Send(data);
                _currentCommand = null;
            }
        }

        private static bool IsSocketDisconnected (Socket socket) {
           // Is there any data available?
            byte[] buffer = null;
            while (socket.Poll(1, SelectMode.SelectRead)) {
                // If no data is available then socket disconnected
                if (socket.Available == 0)
                    return true;

                // Clear socket data; we don't care what InstrumentApp sends
                buffer = buffer ?? new byte[Math.Min(1024, socket.Available)];
                do {
                    socket.Receive(buffer);
                } while (socket.Available != 0);
            }
            return false;
        }

        private void Authenticate (Socket socket) {
            var data = System.Text.Encoding.ASCII.GetBytes(String.Format(
                "hello version 1.0\nauthenticate {0}\n",
                _apiKey
            ));
            socket.Send(data);
        }

        private static Socket Connect () {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect("collector.instrumentalapp.com", 8000);
            return socket;
        }
    }
}
