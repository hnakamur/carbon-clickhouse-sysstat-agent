// Package agent provides an agent which collects system statistics and send them to
// carbon-clickhouse.
//
// The following is the metrics naming rule:
//   sysstat.${serverID}.cpu.user
//   sysstat.${serverID}.cpu.nice
//   sysstat.${serverID}.cpu.sys
//   sysstat.${serverID}.cpu.iowait
//   sysstat.${serverID}.memory.total
//   sysstat.${serverID}.memory.free
//   sysstat.${serverID}.memory.avail
//   sysstat.${serverID}.memory.buffers
//   sysstat.${serverID}.memory.cached
//   sysstat.${serverID}.memory.swap_cached
//   sysstat.${serverID}.memory.swap_total
//   sysstat.${serverID}.memory.swap_free
//   sysstat.${serverID}.disk.${devName}.read_count
//   sysstat.${serverID}.disk.${devName}.read_bytes
//   sysstat.${serverID}.disk.${devName}.written_count
//   sysstat.${serverID}.disk.${devName}.written_bytes
//   sysstat.${serverID}.network.${devName}.recv_bytes
//   sysstat.${serverID}.network.${devName}.recv_packets
//   sysstat.${serverID}.network.${devName}.recv_errs
//   sysstat.${serverID}.network.${devName}.recv_drops
//   sysstat.${serverID}.network.${devName}.trans_bytes
//   sysstat.${serverID}.network.${devName}.trans_packets
//   sysstat.${serverID}.network.${devName}.trans_errs
//   sysstat.${serverID}.network.${devName}.trans_drops
//   sysstat.${serverID}.network.${devName}.trans_colls
//   sysstat.${serverID}.loadavg.load1
//   sysstat.${serverID}.loadavg.load5
//   sysstat.${serverID}.loadavg.load15
//   sysstat.${serverID}.uptime.uptime
package agent