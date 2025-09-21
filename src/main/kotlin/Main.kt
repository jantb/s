import app.App
import app.Channels
import app.QueryChanged
import kafka.Kafka
import kotlinx.coroutines.channels.trySendBlocking
import kube.Kube
import util.UiColors.magenta
import web.WebServer
import widgets.*
import java.awt.*
import java.awt.event.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import javax.swing.ImageIcon
import javax.swing.JFrame
import javax.swing.JPanel
import javax.swing.SwingUtilities
import kotlin.system.exitProcess
import kotlin.time.Duration.Companion.nanoseconds


fun main(args: Array<String>) {
    val useWebUI = args.contains("--web")
    State.useWebUI = useWebUI

    if (State.onMac && !useWebUI) {
        System.setProperty("apple.awt.application.appearance", "dark")
    }

    App().start()
    Kube()
    Kafka()

    if (useWebUI) {
        println("Starting web UI on port 9999")
        WebServer().start()
    } else {
        SwingUtilities.invokeLater {
            val frame = JFrame("Search")
            frame.rootPane.putClientProperty("apple.awt.windowTitleVisible", false)
            val icon = ImageIcon(ClassLoader.getSystemResource("logo.png")).image
            frame.iconImage = icon

            val applicationClass = Class.forName("com.apple.eawt.Application")
            val applicationInstance = applicationClass.getDeclaredConstructor().newInstance()
            val setDockIconMethod = applicationClass.getMethod("setDockIconImage", Image::class.java)
            setDockIconMethod.invoke(applicationInstance, icon)
            val panel = SlidePanel()

            buildViewer(panel)
            buildPodSelect(panel)
            buildKafkaSelect(panel)
            buildKafkaLagView(panel)
            buildLogGroupsView(panel)
            frame.defaultCloseOperation = JFrame.EXIT_ON_CLOSE
            frame.contentPane.add(panel)
            frame.pack()
            frame.setSize(1920, 1280)

            val mousePoint: Point = MouseInfo.getPointerInfo().location

            val frameX = mousePoint.x - frame.width / 2
            val frameY = mousePoint.y - frame.height / 2

            frame.setLocation(frameX, frameY)

            frame.isVisible = true
        }
    }
}


class SlidePanel : JPanel(), KeyListener, MouseListener, MouseWheelListener, MouseMotionListener {
    var componentMap: MutableMap<Mode, MutableList<ComponentOwn>> = mutableMapOf()
    var timer = 0L

    init {
        font = Font("JetBrains Mono", Font.PLAIN, 12)
        addKeyListener(this)
        addMouseListener(this)
        addMouseWheelListener(this)
        addMouseMotionListener(this)
        isFocusable = true
    }

    override fun paintComponent(g: Graphics) {
        super.paintComponent(g)

        when (State.mode) {
            Mode.viewer -> viewer(g)
            Mode.podSelect -> select(g)
            Mode.kafkaSelect -> select(g)
            Mode.kafkaLag -> select(g)
            Mode.logGroups -> select(g)
            Mode.dashboard -> select(g)
        }

        // Draw info line with commands
        g.color = magenta
        val infoString = "Commands: Cmd+P (Pod Select), Cmd+K (Kafka Select), Cmd+G (Kafka Lag), Cmd+I (Log Groups), Cmd+D (Dashboard), Cmd+Q (Quit)"
        val infoBounds = g.fontMetrics.getStringBounds(infoString, g)
        g.drawString(infoString, width - infoBounds.width.toInt() - 10, height - 30)
    }

    private fun viewer(g: Graphics) {
        val componentOwns = componentMap[State.mode]!!

        g.drawImage(
            componentOwns[0].display(width, height - 30, componentOwns[0].x, componentOwns[0].y),
            componentOwns[0].x,
            componentOwns[0].y,
            componentOwns[0].width,
            componentOwns[0].height,
            null
        )
        g.drawImage(
            componentOwns[1].display(width, 30, componentOwns[1].x, height - 30),
            componentOwns[1].x,
            height - 30,
            componentOwns[1].width,
            componentOwns[1].height,
            null
        )

        g.color = magenta
        val timeString =
            "Indexed Lines: ${State.indexedLines.get().format()} Query: ${State.searchTime.get().nanoseconds}"
        val stringBounds = g.fontMetrics.getStringBounds(timeString, g)
        g.drawString(timeString, width - stringBounds.width.toInt() - 10, height - 10)
    }

    private fun select(g: Graphics) {
        val componentOwns = componentMap[State.mode]!!

        // For modal selectors, we only need to display the main component
        g.drawImage(
            componentOwns[0].display(width, height, componentOwns[0].x, componentOwns[0].y),
            componentOwns[0].x,
            componentOwns[0].y,
            componentOwns[0].width,
            componentOwns[0].height,
            null
        )

        g.color = magenta
        val timeString =
            "Kafka time in days:${State.kafkaDays.get()} Indexed Lines: ${State.indexedLines.get().format()} Query: ${State.searchTime.get().nanoseconds}"
        val stringBounds = g.fontMetrics.getStringBounds(timeString, g)
        g.drawString(timeString, width - stringBounds.width.toInt() - 10, height - 10)
    }

    override fun keyPressed(e: KeyEvent) {
        timer = System.nanoTime()

        when {
            e.keyCode == KeyEvent.VK_Q && State.onMac && e.isMetaDown -> exitProcess(0)

            e.keyCode == KeyEvent.VK_ESCAPE -> {
                // For kafkaSelect and podSelect modes, let the modal handle the escape key completely
                // to ensure selected topics/pods start streaming before closing
                if (State.mode == Mode.kafkaSelect || State.mode == Mode.podSelect) {
                    componentMap[State.mode]?.forEach { it.keyPressed(e) }
                    // Don't handle mode switching here - let the modal handle it
                } else {
                    // Escape key returns to viewer mode from other modal views
                    when (State.mode) {
                        Mode.kafkaLag, Mode.logGroups, Mode.dashboard -> {
                            State.mode = Mode.viewer
                            repaint()
                            return
                        }
                        else -> {} // Do nothing for viewer mode
                    }
                }
            }

            (e.isMetaDown && State.onMac || e.isControlDown && !State.onMac) -> when (e.keyCode) {
                KeyEvent.VK_P -> {
                    // Switch to pod select mode
                    State.mode = Mode.podSelect
                    // Pass the key event to the component to trigger modal opening
                    componentMap[State.mode]?.forEach { it.keyPressed(e) }
                    repaint()
                    return
                }

                KeyEvent.VK_K -> {
                    // Switch to kafka select mode
                    State.mode = Mode.kafkaSelect
                    // Pass the key event to the component to trigger modal opening
                    componentMap[State.mode]?.forEach { it.keyPressed(e) }
                    repaint()
                    return
                }

                KeyEvent.VK_G -> {
                    if (State.mode != Mode.kafkaLag) {
                        State.mode = Mode.kafkaLag
                    } else {
                        componentMap[State.mode]?.forEach { it.keyPressed(e) }
                        State.mode = Mode.viewer
                    }
                    repaint()
                }

                KeyEvent.VK_I -> {
                    if (State.mode != Mode.logGroups) {
                        State.mode = Mode.logGroups
                    } else {
                        componentMap[State.mode]?.forEach { it.keyPressed(e) }
                        State.mode = Mode.viewer
                    }
                    repaint()
                }

                KeyEvent.VK_D -> {
                    if (State.mode != Mode.dashboard) {
                        State.mode = Mode.dashboard
                        buildDashboardView(this)
                    } else {
                        componentMap[State.mode]?.forEach { it.keyPressed(e) }
                        State.mode = Mode.viewer
                    }
                    repaint()
                }
            }
        }

        componentMap[State.mode]?.forEach { it.keyPressed(e) }
    }

    override fun keyTyped(e: KeyEvent) {
        timer = System.nanoTime()
        componentMap[State.mode]!!.forEach { it.keyTyped(e) }
    }

    override fun keyReleased(e: KeyEvent) {
        timer = System.nanoTime()
        componentMap[State.mode]!!.forEach { it.keyReleased(e) }
    }

    override fun mouseClicked(e: MouseEvent) {
        timer = System.nanoTime()
        componentMap[State.mode]!!.firstOrNull { mouseInside(e, it) }?.mouseClicked(e)
    }

    override fun mousePressed(e: MouseEvent) {
        timer = System.nanoTime()
        componentMap[State.mode]!!.firstOrNull {
            mouseInside(e, it)
        }?.mousePressed(e)
    }

    override fun mouseReleased(e: MouseEvent) {
        timer = System.nanoTime()
        componentMap[State.mode]!!.firstOrNull { mouseInside(e, it) }?.mouseReleased(e)
    }

    override fun mouseEntered(e: MouseEvent) {
        timer = System.nanoTime()
        componentMap[State.mode]!!.firstOrNull { mouseInside(e, it) }?.mouseEntered(e)
    }

    override fun mouseExited(e: MouseEvent) {
        timer = System.nanoTime()
        componentMap[State.mode]!!.filter { e.x !in it.x..it.width || e.y !in it.y..it.height }
            .forEach { it.mouseExited(e) }
    }

    override fun mouseWheelMoved(e: MouseWheelEvent) {
        timer = System.nanoTime()
        componentMap[State.mode]!!.filter { mouseInside(e, it) }.forEach { it.mouseWheelMoved(e) }
    }

    override fun mouseDragged(e: MouseEvent) {
        timer = System.nanoTime()
        componentMap[State.mode]!!.filter { mouseInside(e, it) }.forEach { it.mouseDragged(e) }
    }

    private fun mouseInside(e: MouseEvent, it: ComponentOwn) = e.x in it.x..it.width && e.y in it.y..(it.y + it.height)

    override fun mouseMoved(e: MouseEvent) {
        componentMap[State.mode]!!.firstOrNull { e.x in it.x..it.width && e.y in it.y..it.height }?.mouseMoved(e)
        componentMap[State.mode]!!.firstOrNull { e.x in it.x..it.width && e.y in it.y..it.height }?.mouseEntered(e)
        componentMap[State.mode]!!.filter { e.x !in it.x..it.width || e.y !in it.y..it.height }
            .forEach { it.mouseExited(e) }
    }
}

private fun buildViewer(panel: SlidePanel) {
    val inputTextLine =
        InputTextLine(panel, 0, 30, panel.width, 30) {
            Channels.searchChannel.trySendBlocking(
                QueryChanged(
                    it,
                    length = State.length.get(),
                    offset = State.offset.get()
                )
            )
        }
    val scrollableList = ScrollableList(
        panel,
        0,
        0,
        panel.width,
        panel.height - 30,
        inputTextLine
    )
    
    panel.componentMap.getOrPut(Mode.viewer) { mutableListOf() } += scrollableList
    panel.componentMap[Mode.viewer]!! += inputTextLine
}

private fun buildPodSelect(panel: SlidePanel) {
    panel.componentMap.getOrPut(Mode.podSelect) { mutableListOf() } += PodSelectModal(
        panel,
        0,
        0,
        panel.width,
        panel.height
    )
}

private fun buildKafkaSelect(panel: SlidePanel) {
    panel.componentMap.getOrPut(Mode.kafkaSelect) { mutableListOf() } += KafkaSelectModal(
        panel,
        0,
        0,
        panel.width,
        panel.height
    )
}

private fun buildKafkaLagView(panel: SlidePanel) {
    panel.componentMap.getOrPut(Mode.kafkaLag) { mutableListOf() } += KafkaLagView(
        panel,
        0,
        0,
        panel.width,
        panel.height - 30
    )
    panel.componentMap.getOrPut(Mode.kafkaLag) { mutableListOf() } += InputTextLine(
        panel,
        0,
        30,
        panel.width,
        30
    ) { }
}

private fun buildLogGroupsView(panel: SlidePanel) {
    panel.componentMap.getOrPut(Mode.logGroups) { mutableListOf() } += LogGroupsView(
        panel,
        0,
        0,
        panel.width,
        panel.height - 30
    )
    panel.componentMap.getOrPut(Mode.logGroups) { mutableListOf() } += InputTextLine(
        panel,
        0,
        30,
        panel.width,
        30
    ) { }
}

private fun buildDashboardView(panel: SlidePanel) {
    panel.componentMap.getOrPut(Mode.dashboard) { mutableListOf() } += DashboardView(
        panel,
        0,
        0,
        panel.width,
        panel.height
    )
}

object State {
    val onMac: Boolean
    val changedAt = AtomicLong(0)
    val kafkaDays = AtomicLong(1)
    val indexedLines = AtomicInteger(0)
    val searchTime = AtomicLong(0)
    val lock = AtomicLong(0)
    val length = AtomicInteger(0)
    val offset = AtomicInteger(0)
    val levels = AtomicReference(run{
        val logLevels = LogLevel.entries.toMutableSet()
        logLevels.remove(LogLevel.UNKNOWN)
        logLevels
    })
    var mode = Mode.viewer
    var useWebUI = false

    init {
        val props = System.getProperties()
        onMac = props["os.name"] == "Mac OS X"
    }
}

enum class Mode {
    viewer,
    podSelect,
    kafkaSelect,
    kafkaLag,
    logGroups,
    dashboard
}

enum class LogLevel {
    INFO, WARN, DEBUG, ERROR, UNKNOWN, KAFKA;
    companion object {
        fun of(value: String?): LogLevel {
            if (value == null) return UNKNOWN
            return try {
                valueOf(value.uppercase())
            } catch (e: IllegalArgumentException) {
                UNKNOWN
            }
        }
        fun fromOrdinal(ordinal: Int): LogLevel {
            return try {
                values()[ordinal]
            } catch (e: IndexOutOfBoundsException) {
                UNKNOWN
            }
        }
    }
}
