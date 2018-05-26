package berlin.intero.sentientlighthub.floorsensor.tasks.scheduled

import berlin.intero.sentientlighthub.common.SentientProperties
import berlin.intero.sentientlighthub.common.services.TinybService
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.logging.Logger

/**
 * This scheduled task scans for GATT devices and displays them
 */
@Component
class GATTScanDevicesScheduledTask {

    companion object {
        private val log: Logger = Logger.getLogger(GATTScanDevicesScheduledTask::class.simpleName)
    }

    @Scheduled(fixedRate = SentientProperties.Frequency.SENSORS_SCAN_RATE)
    fun scanDevices() {
        log.info("${SentientProperties.Color.TASK}-- GATT SCAN SENSORS TASK${SentientProperties.Color.RESET}")

        TinybService.scanDevices()
        TinybService.showDevices(TinybService.scannedDevices)
    }
}
