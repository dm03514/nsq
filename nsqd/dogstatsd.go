package nsqd

import (
	"fmt"
	"time"

	"github.com/nsqio/nsq/internal/statsd"
)

func (n *NSQD) dogStatsdLoop() {
	var lastMemStats *memStats
	var lastStats []TopicStats
	ticker := time.NewTicker(n.getOpts().StatsdInterval)
	for {
		select {
		case <-n.exitChan:
			goto exit
		case <-ticker.C:
			client := statsd.NewDataDogClient(n.getOpts().DogStatsdAddress, n.getOpts().StatsdPrefix)
			err := client.CreateSocket()
			if err != nil {
				n.logf(LOG_ERROR, "failed to create UDP socket to dogstatsd(%s)", client)
				continue
			}

			n.logf(LOG_INFO, "DOGSTATSD: pushing stats to %s", client)

			stats := n.GetStats()
			for _, topic := range stats {
				// try to find the topic in the last collection
				lastTopic := TopicStats{}
				for _, checkTopic := range lastStats {
					if topic.TopicName == checkTopic.TopicName {
						lastTopic = checkTopic
						break
					}
				}
				diff := topic.MessageCount - lastTopic.MessageCount
				// stat := fmt.Sprintf("nsq.message_count", topic.TopicName)
				client.Incr("nsq.message_count", int64(diff), map[string]string{
					"topic_name": topic.TopicName,
				})

				client.Gauge("topic.depth", topic.Depth, map[string]string{
					"topic_name": topic.TopicName,
				})

				stat = fmt.Sprintf("topic.%s.backend_depth", topic.TopicName)
				client.Gauge(stat, topic.BackendDepth)

				for _, item := range topic.E2eProcessingLatency.Percentiles {
					stat = fmt.Sprintf("topic.%s.e2e_processing_latency_%.0f", topic.TopicName, item["quantile"]*100.0)
					// We can cast the value to int64 since a value of 1 is the
					// minimum resolution we will have, so there is no loss of
					// accuracy
					client.Gauge(stat, int64(item["value"]))
				}

				for _, channel := range topic.Channels {
					// try to find the channel in the last collection
					lastChannel := ChannelStats{}
					for _, checkChannel := range lastTopic.Channels {
						if channel.ChannelName == checkChannel.ChannelName {
							lastChannel = checkChannel
							break
						}
					}
					diff := channel.MessageCount - lastChannel.MessageCount
					stat := fmt.Sprintf("topic.%s.channel.%s.message_count", topic.TopicName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					stat = fmt.Sprintf("topic.%s.channel.%s.depth", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, channel.Depth)

					stat = fmt.Sprintf("topic.%s.channel.%s.backend_depth", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, channel.BackendDepth)

					stat = fmt.Sprintf("topic.%s.channel.%s.in_flight_count", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, int64(channel.InFlightCount))

					stat = fmt.Sprintf("topic.%s.channel.%s.deferred_count", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, int64(channel.DeferredCount))

					diff = channel.RequeueCount - lastChannel.RequeueCount
					stat = fmt.Sprintf("topic.%s.channel.%s.requeue_count", topic.TopicName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					diff = channel.TimeoutCount - lastChannel.TimeoutCount
					stat = fmt.Sprintf("topic.%s.channel.%s.timeout_count", topic.TopicName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					stat = fmt.Sprintf("topic.%s.channel.%s.clients", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, int64(len(channel.Clients)))

					for _, item := range channel.E2eProcessingLatency.Percentiles {
						stat = fmt.Sprintf("topic.%s.channel.%s.e2e_processing_latency_%.0f", topic.TopicName, channel.ChannelName, item["quantile"]*100.0)
						client.Gauge(stat, int64(item["value"]))
					}
				}
			}
			lastStats = stats

			if n.getOpts().StatsdMemStats {
				ms := getMemStats()

				client.Gauge("mem.heap_objects", int64(ms.HeapObjects))
				client.Gauge("mem.heap_idle_bytes", int64(ms.HeapIdleBytes))
				client.Gauge("mem.heap_in_use_bytes", int64(ms.HeapInUseBytes))
				client.Gauge("mem.heap_released_bytes", int64(ms.HeapReleasedBytes))
				client.Gauge("mem.gc_pause_usec_100", int64(ms.GCPauseUsec100))
				client.Gauge("mem.gc_pause_usec_99", int64(ms.GCPauseUsec99))
				client.Gauge("mem.gc_pause_usec_95", int64(ms.GCPauseUsec95))
				client.Gauge("mem.next_gc_bytes", int64(ms.NextGCBytes))
				client.Incr("mem.gc_runs", int64(ms.GCTotalRuns-lastMemStats.GCTotalRuns))

				lastMemStats = ms
			}

			client.Close()
		}
	}

exit:
	ticker.Stop()
}
