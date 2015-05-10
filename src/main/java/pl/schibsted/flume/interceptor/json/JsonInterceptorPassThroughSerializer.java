/*
 * Copyright 2015 Schibsted Tech Polska Sp. z o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.schibsted.flume.interceptor.json;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;

public class JsonInterceptorPassThroughSerializer implements JsonInterceptorSerializer {


    @Override
    public String serialize(String value) {
        return value;
    }

    @Override
    public void configure(Context context) {
    }

    @Override
    public void configure(ComponentConfiguration conf) {
    }

}
