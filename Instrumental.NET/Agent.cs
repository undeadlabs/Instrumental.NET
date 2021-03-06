﻿//  Copyright 2014 Bloomerang
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
using System.Text.RegularExpressions;

namespace Instrumental.NET {
    public class Agent {
        private readonly Collector _collector;
        private readonly string _prefix;
        private readonly Regex _validateMetric;

        public Agent (String apiKey, string prefix = null) {
            _validateMetric = new Regex(
                @"^[\d\w\-_]+(\.[\d\w\-_]+)+$",
                RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.ExplicitCapture
            );

            if (!string.IsNullOrEmpty(apiKey))
                _collector = new Collector(apiKey);

            if (!string.IsNullOrEmpty(prefix))
                ValidateMetricName(prefix + "fake");
            _prefix = prefix ?? "";
        }

        public void Gauge (String metricName, float value) {
            ValidateMetricName(metricName);
            if (_collector == null) return;
            _collector.SendMessage(String.Format("gauge {0}{1} {2} {3}\n", _prefix, metricName, value, DateTime.Now.ToEpoch()));
        }

        public void GaugeAbsolute (String metricName, float value) {
            ValidateMetricName(metricName);
            if (_collector == null) return;
            _collector.SendMessage(String.Format("gauge_absolute {0}{1} {2} {3}\n", _prefix, metricName, value, DateTime.Now.ToEpoch()));
        }

        public void Increment (String metricName, float value = 1) {
            ValidateMetricName(metricName);
            if (_collector == null) return;
            _collector.SendMessage(String.Format("increment {0}{1} {2} {3}\n", _prefix, metricName, value, DateTime.Now.ToEpoch()));
        }

        /// <summary>
        /// Record a notice, which DOES NOT include the Agent prefix string set upon initialization
        /// </summary>
        /// <param name="message">A string describing the event</param>
        /// <param name="duration">The duration of the event. You may specify 0 for events which have no specific duration.</param>
        /// <param name="time">The time when the event occurred</param>
        public void Notice (String message, float duration = 0) {
            ValidateNote(message);
            if (_collector == null) return;
            _collector.SendMessage(String.Format("notice {0} {1} {2}\n", DateTime.Now.ToEpoch(), duration, message));
        }

        private static void ValidateNote (String message) {
            var valid = message.IndexOf("\r") == -1 && message.IndexOf("\n") == -1;
            if (!valid)
                throw new InstrumentalException("Invalid notice message: {0}", message);
        }

        private void ValidateMetricName (String metricName) {
            if (!_validateMetric.IsMatch(metricName))
                throw new InstrumentalException("Invalid metric name: {0}", metricName);
        }
    }

    public class InstrumentalException : Exception {
        public InstrumentalException (string message) : base(message) { }
        public InstrumentalException (string format, params object[] args) : base(string.Format(format, args)) { }
    }
}
