import app.App
import kube.Kube
import slides.SlideColors.magenta
import widgets.InputTextLine
import widgets.PodSelect
import widgets.ScrollableList
import java.awt.Desktop
import java.awt.Graphics
import java.awt.event.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import javax.swing.ImageIcon
import javax.swing.JFrame
import javax.swing.JPanel
import javax.swing.SwingUtilities
import kotlin.system.exitProcess
import kotlin.time.Duration.Companion.nanoseconds


class SlidePanel : JPanel(), KeyListener, MouseListener, MouseWheelListener, MouseMotionListener {
    var componentMap: MutableMap<Mode, MutableList<ComponentOwn>> = mutableMapOf()
    var timer = 0L

    init {
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
            Mode.podSelect -> {podSelect(g)}
            Mode.kafkaSelect -> {}
        }
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

    private fun podSelect(g: Graphics) {
        val componentOwns = componentMap[State.mode]!!

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
            "Indexed Lines: ${State.indexedLines.get().format()} Query: ${State.searchTime.get().nanoseconds}"
        val stringBounds = g.fontMetrics.getStringBounds(timeString, g)
        g.drawString(timeString, width - stringBounds.width.toInt() - 10, height - 10)
    }

    override fun keyPressed(e: KeyEvent) {
        timer = System.nanoTime()

        if (e.keyCode == KeyEvent.VK_Q && State.onMac && e.isMetaDown) {
            exitProcess(0)
        }

        if (((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) && e.keyCode == KeyEvent.VK_P) {
            if (State.mode != Mode.podSelect) {
                State.mode = Mode.podSelect
                componentMap[State.mode]!!.forEach { it.keyPressed(e) }
            } else if (State.mode == Mode.podSelect) {
                State.mode = Mode.viewer
                componentMap[State.mode]!!.forEach { it.keyPressed(e) }
            }
            repaint()
        }
        componentMap[State.mode]!!.forEach { it.keyPressed(e) }
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

fun main() {
    if (State.onMac) {
        System.setProperty( "apple.awt.application.appearance", "dark" )
    }

    App().start()
    Kube()
    SwingUtilities.invokeLater {
        val frame = JFrame("Search")
        frame.rootPane.putClientProperty( "apple.awt.windowTitleVisible", false );
        val icon = ImageIcon("logo.png").image

        frame.iconImage = icon

        val panel = SlidePanel()
        buildViewer(panel)
        buildPodSelect(panel)
        frame.defaultCloseOperation = JFrame.EXIT_ON_CLOSE
        frame.contentPane.add(panel)
        frame.pack()
        frame.setSize(800, 600)
        frame.isVisible = true
    }
}

private fun buildViewer(panel: SlidePanel) {
    val inputTextLine = InputTextLine(panel, 0, 30, panel.width, 30)
    panel.componentMap.getOrPut(Mode.viewer) { mutableListOf() } += ScrollableList(
        panel,
        0,
        0,
        panel.width,
        panel.height - 30,
        inputTextLine
    )
    panel.componentMap[Mode.viewer]!! += inputTextLine
}
private fun buildPodSelect(panel: SlidePanel) {
    panel.componentMap.getOrPut(Mode.podSelect) { mutableListOf() } += PodSelect(panel, 0, 0, panel.width, panel.height)
}

object State {
    val onMac: Boolean
    val changedAt = AtomicLong(0)
    val indexedLines = AtomicInteger(0)
    val searchTime = AtomicLong(0)
    var mode = Mode.viewer

    init {
        val props = System.getProperties()
        onMac = props["os.name"] == "Mac OS X"
    }
}

enum class Mode {
    viewer,
    podSelect,
    kafkaSelect
}
