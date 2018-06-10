package berlin.intero.sentientlighthub.floorsensor.tasks.scheduled

import berlin.intero.sentientlighthub.common.SentientProperties
import berlin.intero.sentientlighthub.common.model.MQTTEvent
import berlin.intero.sentientlighthub.common.services.ConfigurationService
import berlin.intero.sentientlighthub.common.services.SentientParserService
import berlin.intero.sentientlighthub.common.services.TinybService
import berlin.intero.sentientlighthub.common.tasks.MQTTPublishAsyncTask
import berlin.intero.sentientlighthubplayground.exceptions.BluetoothConnectionException
import com.google.gson.Gson
import org.springframework.core.task.SyncTaskExecutor
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import tinyb.BluetoothException
import java.util.logging.Logger

/**
 * This scheduled task
 * <li> reads GATT characteristics
 * <li> calls {@link MQTTPublishAsyncTask} to publish the characteristics' values to a MQTT broker
 */
@Component
class GATTReadSensorScheduledTask {

    companion object {
        private val log: Logger = Logger.getLogger(GATTReadSensorScheduledTask::class.simpleName)

        private var counter = 0
        private var runtimeMin = Long.MAX_VALUE
        private var runtimeMax = Long.MIN_VALUE
    }

    @Scheduled(fixedDelay = SentientProperties.Frequency.SENSOR_READ_DELAY)
    fun readSensor() {
        val startTime = System.currentTimeMillis()
        log.info("${SentientProperties.Color.TASK}-- GATT READ SENSOR TASK${SentientProperties.Color.RESET}")

        val scannedDevices = TinybService.scannedDevices
        val intendedDevices = ConfigurationService.sensorConfig?.sensorDevices

        log.fine("Show scannedDevices ${Gson().toJson(scannedDevices.map { d -> d.address })}")
        log.fine("Show intendedDevices ${Gson().toJson(intendedDevices?.map { d -> d.address })}")

        if (scannedDevices.isEmpty()) {
            GATTScanDevicesScheduledTask().scanDevices()
        }

        // Iterate over intended devices being enabled
        intendedDevices?.filter { d -> d.enabled }?.forEach { intendedDevice ->
            try {
                log.info("Intended device ${intendedDevice.address}")

                val device = scannedDevices.first { d -> d.address == intendedDevice.address }

                // Ensure connection
                TinybService.ensureConnection(device)

                // Show services
                // TinybController.showServices(device)

                // Read raw value
                val rawValue = TinybService.readCharacteristic(device, SentientProperties.GATT.Characteristic.SENSOR)

                // Parse values
                val parsedValues = SentientParserService.parse(rawValue)

                // Assemble values to be published
                val mqttEvents = ArrayList<MQTTEvent>()
                intendedDevice.cables.forEachIndexed { cableIndex, cable ->

                    cable.sensors.forEachIndexed { sensorIndex, sensor ->

                        if (cable.enabled && sensor.enabled) {
                            val topic = "${SentientProperties.MQTT.Topic.SENSOR}/${sensor.checkerboardID}"
                            val value = parsedValues.getOrElse((cableIndex * SentientProperties.MAX_SENSORS_PER_CABLE) + sensorIndex + 1) { SentientProperties.GATT.INVALID_VALUE }

                            val mqttEvent = MQTTEvent(topic, value.toString())

                            if (value != SentientProperties.GATT.INVALID_VALUE) {
                                mqttEvents.add(mqttEvent)
                            }
                        }
                    }
                }

                // Publish values
                if (mqttEvents.isNotEmpty()) {
                    // Call MQTTPublishAsyncTask
                    SyncTaskExecutor().execute(MQTTPublishAsyncTask(mqttEvents))
                } else {
                    log.info(".")
                    Thread.sleep(SentientProperties.Frequency.UNSUCCESSFUL_TASK_DELAY)
                }
            } catch (ex: Exception) {
                when (ex) {
                    is BluetoothException -> {
                        log.severe("Generic bluetooth exception")
                    }
                    is BluetoothConnectionException -> {
                        log.severe("Cannot connect to device ${intendedDevice.address}")
                    }
                    is NoSuchElementException -> {
                        log.severe("Cannot find device ${intendedDevice.address}")
                    }
                    else -> throw ex
                }
            }
        }

        val endTime = System.currentTimeMillis()
        val runtime = endTime - startTime
        if (counter > 0) {
            if (runtime < runtimeMin) runtimeMin = runtime
            if (runtime > runtimeMax) runtimeMax = runtime
        }

        log.info("-- End counter ${++counter} / ${runtime} millis / min ${runtimeMin} / max ${runtimeMax}")
    }
}
