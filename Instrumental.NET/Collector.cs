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

namespace Instrumental.NET
{
    class Collector
    {
        private const int MaxBuffer = 5000;
        private const int Backoff = 2;
        private const int MaxReconnectDelay = 15;

        private readonly string _apiKey;
        private readonly BlockingCollection<String> _messages = new BlockingCollection<String>();
        private String _currentCommand;
        private Thread _worker;
        private bool _queueFullWarned;
        private static readonly Logger _log = LogManager.GetCurrentClassLogger();

        public Collector(String apiKey)
        {
            _apiKey = apiKey;
            _worker = new Thread(WorkerLoop) {IsBackground = true};
            _worker.Start();
        }

        public void SendMessage(String message, bool synchronous)
        {
            // Make sure the message is terminated with "\n" and includes no other "\r\n" characters
            if (message.IndexOf("\r") != -1 || message.IndexOf("\n") != message.Length - 1)
                throw new InstrumentalException("Invalid message, {0}", message);

            if (synchronous)
            {
                // Blocks if queue full
                _messages.Add(message);
            }
            else if (_messages.TryAdd(message))
            {
                if (_queueFullWarned)
                {
                    _queueFullWarned = false;
                    _log.Info("Queue available again");
                }
            }
            else
            {
                if (!_queueFullWarned)
                {
                    _queueFullWarned = true;
                    _log.Warn("Queue full; dropping messages until there's room");
                }
            }
        }

        private void WorkerLoop()
        {
            while (true)
            {
                Socket socket = null;
                var failures = 0;

                try
                {
                    socket = Connect();
                    Authenticate(socket);
                    failures = 0;
                    SendQueuedMessages(socket);
                }
                catch (Exception e)
                {
                    _log.Error("An exception occurred", e);
                    if (socket != null)
                    {
                        socket.Disconnect(false);
                        socket = null;
                    }
                    var delay = (int) Math.Min(MaxReconnectDelay, Math.Pow(failures++, Backoff));
                    _log.Error("Disconnected. {0} failures in a row. Reconnect in {1} seconds.", failures, delay);
                    Thread.Sleep(delay*1000);
                }
            }
        }

        private void SendQueuedMessages(Socket socket)
        {
            while (true)
            {
                if (_currentCommand == null) _currentCommand = _messages.Take();

                // if (socket.Poll(1, SelectMode.SelectRead) && socket.Available == 0)
                //    throw new Exception("Disconnected");

                var data = System.Text.Encoding.ASCII.GetBytes(_currentCommand);
                socket.Send(data);
                _currentCommand = null;
            }
        }

        private void Authenticate(Socket socket)
        {
            var data = System.Text.Encoding.ASCII.GetBytes(String.Format(
                "hello version 1.0\nauthenticate {0}\n",
                _apiKey
            ));
            socket.Send(data);
        }

        private static Socket Connect()
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect("collector.instrumentalapp.com", 8000);
            return socket;
        }
    }
}
