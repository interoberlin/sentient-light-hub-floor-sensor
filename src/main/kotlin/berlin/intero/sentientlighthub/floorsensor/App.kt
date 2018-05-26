package berlin.intero.sentientlighthub.floorsensor

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling
import java.util.logging.Logger

@SpringBootApplication
@EnableScheduling
class App

fun main(args: Array<String>) {
    runApplication<App>(*args)

    val log = Logger.getLogger(App::class.simpleName)

    log.info("Sentient Light Hub Floor Sensor")
}
