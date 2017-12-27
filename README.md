# flume-json-interceptor

Flume JSON Interceptor Plugin extend Flume NG - now you can add headers getted from JSON document throught JSONPath.

JSONPath is XPath for JSON. Specyfication: http://goessner.net/articles/JsonPath/ 

## Compilation

You can compile these using Maven (which you have to install first) by running

```
mvn clean package
```

from your command prompt.

## Installation

Extract file `flume-json-interceptor-x.y.z-flume-plugin.tar` to directory: `plugins.d/flume-json-interceptor/`.

If you use Cloudera Distrybution Hadoop, this will probably be `/usr/lib/flume-ng/plugins.d/plugins.d/flume-json-interceptor/`.

## Configuration

Simple configuration to get element from `action` property and put it as header `action`.

Example:

```
a1.sources.s1.interceptors = i1
a1.sources.s1.interceptors.i1.type = pl.schibsted.flume.interceptor.json.JsonInterceptor$Builder
a1.sources.s1.interceptors.i1.name = action
a1.sources.s1.interceptors.i1.jsonpath = $.action
```

Configuration to get element from `published` property and put it as header `timestamp` throught serializer.

Example:

```
a1.sources.s1.interceptors = i2
a1.sources.s1.interceptors.i2.type = pl.schibsted.flume.interceptor.json.JsonInterceptor$Builder
a1.sources.s1.interceptors.i2.name = timestamp
a1.sources.s1.interceptors.i2.jsonpath = $.published
a1.sources.s1.interceptors.i2.serializers = dt
a1.sources.s1.interceptors.i2.serializers.dt.type=pl.schibsted.flume.interceptor.json.JsonInterceptorMillisSerializer
a1.sources.s1.interceptors.i2.serializers.dt.pattern=yyyy-MM-dd'T'HH:mm:ssZ
a1.sources.s1.interceptors.i2.serializers.dt.name=timestamp
```

Example JSON document:

```
{
  action: "pageview",
  published: "2015-05-06T12:34:54+02:00"
}
```

## Error handling

1. If specified JSONPath element not exists, event is passed without modifications.

2. If JSON is malformed, event is passed without modifications.




