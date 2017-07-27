const Bacon = require('baconjs');
const _ = require('lodash');
const WebSocketClient = require('websocket').client;

const socketUrl = 'ws://stream.meetup.com/2/rsvps'; // gotta stream *something*

require('colors'); // changes String prototype

async function stream() {
    const connection = await getConnection(socketUrl);

    // take the JSON-stringified sockets messages
    // and create a bacon stream with JS objects
    const eventStream = Bacon
        // connection is an EventEmitter
        // upon each 'message' event, add it to the bacon event stream
        .fromEvent(connection, 'message')

        // try to parse the JSON data into JS objects
        .map(e => {
            try {
                return JSON.parse(e.utf8Data);
            } catch (error) {
                return undefined;
            }
        })

        // filter out empty events
        .filter(event => !!event)

    // create stream of rsvp data
    const rsvps = eventStream
        // pick out args with interested in
        .map(({
            member: { member_name },
            response,
            event: { event_name },
            group: { group_city },
        }) => ({ member_name, response, event_name, group_city }));

    // create stream of top 5 cities
    const topFiveCities = eventStream
        // scan is like a running reduce
        // it produces a new reduced value for each event on the stream
        .scan({}, (memo, { group: { group_city } }) => {
            memo[group_city] = memo[group_city] + 1 || 1;
            return memo;
        })

        // cities is a complete hashmap of all cities (keys) and counts (values)
        // convert this hashmap to the top 5 cities with highest count (sorted desc)
        .map(cities => {
            return _
                .chain(cities)
                .toPairs()
                .map(([city, count]) => ({ city, count }))
                .sortBy(({ count }) => -count)
                .slice(0, 5)
                .reduce((memo, { city, count }) => `${memo}${city}: ${count}\n`, '')
                .value();
        })

        // filter out empty top5 (when no cities have been streamed yet)
        .filter(top5 => !!top5)

    // map the streams to (loggable) strings and merge them into a single stream
    const merged = Bacon.mergeAll(
        rsvps
            // convert it to something loggable
            .map(({ member_name, response, event_name, group_city }) => (
                `${member_name} rsvp'd ${coloredResponse(response)} to ${event_name.white.bold} in ${group_city.gray}`
            )),
        topFiveCities
            // we only want an top5 update every few seconds
            .throttle(10 * 1000)

            // convert it to something loggable
            .map(top5 => top5 && `\n${' Top 5 '.underline.bgYellow.black}\n${top5}`)
    );

    // log merged events
    merged.onValue(console.log);
}

try {
    stream();
} catch(e) {
    console.log('***** e', e)
}

function getConnection(socketUrl) {
    return new Promise(resolve => {
        const client = new WebSocketClient();

        client.connect(socketUrl);

        client.on('connectFailed', function(error) {
            process.exit('Connect Error: ' + error.toString());
        });

        client.on('connect', connection => {
            console.log('WebSocket Client Connected');

            resolve(connection);

            connection.on('error', function(error) {
                process.exit("Connection Error: " + error.toString());
            });
            connection.on('close', function() {
                process.exit('echo-protocol Connection Closed');
            });
        });
    });
}

function coloredResponse(response) {
    if (response === 'yes') return response.green.bold;
    if (response === 'no') return response.red.bold;
    return response;
}
