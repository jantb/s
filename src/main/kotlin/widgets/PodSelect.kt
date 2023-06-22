package widgets

import ComponentOwn
import SlidePanel
import State
import app.*
import round
import slides.SlideColors
import slides.Styles
import java.awt.Font
import java.awt.Graphics2D
import java.awt.event.*
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import kotlin.math.abs

class PodSelect(private val panel: SlidePanel, x: Int, y: Int, width: Int, height: Int) : ComponentOwn(),
    KeyListener,
    MouseListener, MouseWheelListener,
    MouseMotionListener {
    private val items: MutableList<Item> = mutableListOf()

    init {
        this.x = x
        this.y = y
        this.height = height
        this.width = width
        this.items.clear()
    }

    private var selectedLineIndex = 0
    private var rowHeight = 12
    private lateinit var image: BufferedImage
    private lateinit var g2d: Graphics2D
    private var selectedTextRange: IntRange? = null
    private var mouseposX = 0
    private var mouseposY = 0
    private var mouseposXPressed = 0
    private var mouseposYPressed = 0
    private var mouseposXReleased = 0
    private var mouseposYReleased = 0
    private lateinit var maxCharBounds: Rectangle2D
    private var selectedText = ""
    var text = ""
    var cursorIndex = 0
    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        if (this.width != width || this.height != height || this.x != x || this.y != y) {
            if (::g2d.isInitialized) {
                this.g2d.dispose()
            }
            this.image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
            this.g2d = this.image.createGraphics()
            this.image
            this.height = height
            this.width = width
            this.x = x
            this.y = y
        }
        //Clear
        g2d.color = SlideColors.background
        g2d.fillRect(0, 0, width, height)
        g2d.font = Font(Styles.normalFont, Font.PLAIN, rowHeight)
        maxCharBounds = g2d.fontMetrics.getMaxCharBounds(g2d)

        g2d.color = SlideColors.magenta
        drawSelectedLine()
        paint()
        return image
    }

    override fun repaint(componentOwn: ComponentOwn) {
        TODO("Not yet implemented")
    }

    private fun paint() {
        g2d.color = SlideColors.defaultText
        items.mapIndexed { index, it ->
            g2d.color = if (it.selected) SlideColors.green else SlideColors.defaultText
            g2d.drawString(it.name, 0, maxCharBounds.height.toInt() * (index + 1))
        }
    }

    private fun drawSelectedLine() {
        g2d.color = SlideColors.selectionLine
        val height = maxCharBounds.height.toInt() + g2d.fontMetrics.maxDescent
        g2d.fillRect(0, maxCharBounds.height.toInt() * selectedLineIndex, width, height)
    }

    override fun keyTyped(e: KeyEvent) {

    }

    var namespace = ""
    override fun keyPressed(e: KeyEvent) {
        if (((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) && e.keyCode == KeyEvent.VK_A) {
            items.forEach { item ->
                item.selected = !item.selected
                if (item.selected) {
                    Channels.podsChannel.put(ListenToPod( podName = item.name))
                } else {
                    Channels.podsChannel.put(UnListenToPod( podName = item.name))
                }
            }
            panel.repaint()
        } else if (((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) && e.keyCode == KeyEvent.VK_P) {
            Channels.podsChannel.put(UnListenToPods)
            Channels.cmdChannel.put(ClearIndex)
            val listPods = ListPods()
            Channels.podsChannel.put(listPods)
            items.clear()
            items.addAll(listPods.result.get().map { Item(it, false) })
            selectedLineIndex = 0
            panel.repaint()
        } else if (e.keyCode == KeyEvent.VK_DOWN) {
            selectedLineIndex++
            selectedLineIndex = selectedLineIndex.coerceIn(0 until items.size)
            panel.repaint()
        } else if (e.keyCode == KeyEvent.VK_UP) {
            selectedLineIndex--
            selectedLineIndex = selectedLineIndex.coerceIn(0 until items.size)
            panel.repaint()
        } else if (e.keyCode == KeyEvent.VK_ENTER) {
            val item = items[selectedLineIndex]
            item.selected = !item.selected
            if (item.selected) {
                Channels.podsChannel.put(ListenToPod( podName = item.name))
            } else {
                Channels.podsChannel.put(UnListenToPod( podName = item.name))
            }
            panel.repaint()
        } else if (e.keyCode == KeyEvent.VK_P && State.onMac && e.isMetaDown) {
            panel.repaint()
        }

    }

    override fun keyReleased(e: KeyEvent) {
        panel.repaint()
    }


    override fun mouseClicked(e: MouseEvent) {

    }


    override fun mousePressed(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7
        mouseposXPressed = mouseposX
        mouseposYPressed = mouseposY
        mouseposXReleased = mouseposX
        mouseposYReleased = mouseposY
        selectedText = ""
        panel.repaint()
    }

    override fun mouseReleased(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y
        mouseposXReleased = mouseposX
        mouseposYReleased = mouseposY
        selectedText = getSelectedTextFromMouseIndex(
            text,
            mouseposXPressed,
            mouseposXReleased
        )
        val a = getCharIndexFromMouse(text, mouseposXPressed)
        val b = getCharIndexFromMouse(text, mouseposXReleased)

        val sorted = listOf(a, b).sorted()
        selectedTextRange = (sorted[0]..sorted[1])
        panel.repaint()
    }

    private fun getSelectedTextFromMouseIndex(string: String, mouseXPressed: Int, mouseXReleased: Int): String {
        val stringWidth = g2d.fontMetrics.stringWidth(string)
        val x =
            (mouseXPressed - if (mouseXReleased - mouseXPressed > 0) 0 else abs(mouseXReleased - mouseXPressed)).round(
                maxCharBounds.width.toInt()
            ).coerceIn(0..stringWidth)
        val width = abs(mouseXReleased - mouseXPressed).round(
            maxCharBounds.width.toInt()
        ).coerceIn(0..stringWidth - x)
        var s = ""
        var res = ""
        string.forEach { c ->
            s += c
            val sw = g2d.fontMetrics.stringWidth(s)
            if (sw > x && sw <= x + width) {
                res += c
            }
        }
        return res
    }

    private fun getCharIndexFromMouse(string: String, x: Int): Int {
        var s = ""
        string.forEachIndexed { i, c ->
            s += c
            val sw = g2d.fontMetrics.stringWidth(s)
            if (sw > x) {
                return i
            }
        }
        return string.lastIndex
    }

    override fun mouseEntered(e: MouseEvent) {
        if (!mouseInside) {
            mouseInside = true
        }
    }

    override fun mouseExited(e: MouseEvent) {
        if (mouseInside) {
            mouseInside = false
        }
    }

    override fun mouseWheelMoved(e: MouseWheelEvent) {
        if ((e.isControlDown && !State.onMac) || (e.isMetaDown && State.onMac)) {
            rowHeight += e.wheelRotation
            rowHeight = rowHeight.coerceIn(1..100)
            mouseposXPressed = 0
            mouseposYPressed = 0
            mouseposXReleased = 0
            mouseposYReleased = 0
            panel.repaint()
            return
        }


        panel.repaint()
    }

    override fun mouseDragged(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7
        mouseposXReleased = mouseposX
        mouseposYReleased = mouseposY
        cursorIndex = getCharIndexFromMouse(text, mouseposX)
        panel.repaint()
    }

    override fun mouseMoved(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7

        panel.repaint()
    }

    enum class PodSelectState {
        nameSpace, podName
    }

    data class Item(
        val name: String,
        var selected: Boolean
    )
}
