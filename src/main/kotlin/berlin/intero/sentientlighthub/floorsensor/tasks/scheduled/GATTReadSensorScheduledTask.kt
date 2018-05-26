package berlin.intero.sentientlighthub.floorsensor.tasks.scheduled

import berlin.intero.sentientlighthub.common.SentientProperties
import berlin.intero.sentientlighthub.common.services.ConfigurationService
import berlin.intero.sentientlighthub.common.services.SentientParserService
import berlin.intero.sentientlighthub.common.services.TinybService
import berlin.intero.sentientlighthub.common.tasks.MQTTPublishAsyncTask
import berlin.intero.sentientlighthubplayground.exceptions.BluetoothConnectionException
import com.google.gson.Gson
import org.springframework.core.task.SimpleAsyncTaskExecutor
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
    }

    @Scheduled(fixedDelay = SentientProperties.Frequency.SENSOR_READ_DELAY)
    fun readSensor() {
        log.info("${SentientProperties.Color.TASK}-- GATT READ SENSOR TASK${SentientProperties.Color.RESET}")

        val scannedDevices = TinybService.scannedDevices
        val intendedDevices = ConfigurationService.sensorConfig?.sensorDevices

        log.fine("Show scannedDevices ${Gson().toJson(scannedDevices.map { d -> d.address })}")
        log.fine("Show intendedDevices ${Gson().toJson(intendedDevices?.map { d -> d.address })}")

        if (scannedDevices.isEmpty()) {
            GATTScanDevicesScheduledTask().scanDevices()
        }

        // Iterate over intended devices
        intendedDevices?.forEach { intendedDevice ->
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

                // Publish values
                intendedDevice.sensors.forEach { s ->
                    val topic = "${SentientProperties.MQTT.Topic.SENSOR}/${s.checkerboardID}"
                    val value = parsedValues.getOrElse(s.index) { SentientProperties.GATT.INVALID_VALUE }

                    log.fine("${SentientProperties.Color.VALUE} ${s.checkerboardID} -> ${value} ${SentientProperties.Color.RESET}")

                    if (value != SentientProperties.GATT.INVALID_VALUE) {
                        // Call MQTTPublishAsyncTask
                        SimpleAsyncTaskExecutor().execute(MQTTPublishAsyncTask(topic, value.toString()))
                    }
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
    }
}
