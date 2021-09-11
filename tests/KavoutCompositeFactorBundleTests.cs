/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using System;
using ProtoBuf;
using System.IO;
using System.Linq;
using ProtoBuf.Meta;
using Newtonsoft.Json;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.DataSource;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class KavoutCompositeFactorBundleTests 
    {
        [Test]
        public void ReaderTest()
        {
            var factory = new KavoutCompositeFactorBundle();
            var config = new SubscriptionDataConfig(
                typeof(object),
                Symbol.Create("AAPL", SecurityType.Equity, Market.USA),
                Resolution.Daily,
                TimeZones.NewYork,
                TimeZones.NewYork,
                false,
                false,
                false,
                true);

            var line = "20210909,4,5,6,7,8";
            var expected = (KavoutCompositeFactorBundle) CreateNewInstance();
            var actual = (KavoutCompositeFactorBundle) factory.Reader(config, line, new DateTime(2021, 9, 10), false);
            
            Assert.AreEqual(expected.Growth, actual.Growth);
            Assert.AreEqual(expected.ValueFactor, actual.ValueFactor);
            Assert.AreEqual(expected.Quality, actual.Quality);
            Assert.AreEqual(expected.Momentum, actual.Momentum);
            Assert.AreEqual(expected.LowVolatility, actual.LowVolatility);
            
            Assert.AreEqual(expected.Time, actual.Time);
            Assert.AreEqual(expected.EndTime, actual.EndTime);
            Assert.AreEqual(expected.Symbol, actual.Symbol);
            
        }
        
        [Test]
        public void JsonRoundTrip()
        {
            var expected = CreateNewInstance();
            var type = expected.GetType();
            var serialized = JsonConvert.SerializeObject(expected);
            var result = JsonConvert.DeserializeObject(serialized, type);

            AssertAreEqual(expected, result);
        }

        [Test]
        public void ProtobufRoundTrip()
        {
            var expected = CreateNewInstance();
            var type = expected.GetType();

            RuntimeTypeModel.Default[typeof(BaseData)].AddSubType(2000, type);

            using (var stream = new MemoryStream())
            {
                Serializer.Serialize(stream, expected);

                stream.Position = 0;

                var result = Serializer.Deserialize(type, stream);

                AssertAreEqual(expected, result, filterByCustomAttributes: true);
            }
        }

        [Test]
        public void Clone()
        {
            var expected = CreateNewInstance();
            var result = expected.Clone();

            AssertAreEqual(expected, result);
        }

        private void AssertAreEqual(object expected, object result, bool filterByCustomAttributes = false)
        {
            foreach (var propertyInfo in expected.GetType().GetProperties())
            {
                // we skip Symbol which isn't protobuffed
                if (filterByCustomAttributes && propertyInfo.CustomAttributes.Count() != 0)
                {
                    Assert.AreEqual(propertyInfo.GetValue(expected), propertyInfo.GetValue(result));
                }
            }
            foreach (var fieldInfo in expected.GetType().GetFields())
            {
                Assert.AreEqual(fieldInfo.GetValue(expected), fieldInfo.GetValue(result));
            }
        }

        private BaseData CreateNewInstance()
        {
            return new KavoutCompositeFactorBundle
            {
                Growth = 4m,
                ValueFactor = 5m,
                Quality = 6m,
                Momentum = 7m,
                LowVolatility = 8m,
                
                Time = new DateTime(2021, 9, 9),
                EndTime = new DateTime(2021, 9, 10),
                Symbol = Symbol.Create("AAPL", SecurityType.Equity, Market.USA)
            };
        }
    }
}
