package widgets

import ComponentOwn
import SlidePanel
import State
import round
import util.Styles
import util.UiColors
import java.awt.Font
import java.awt.Graphics2D
import java.awt.Toolkit
import java.awt.datatransfer.Clipboard
import java.awt.datatransfer.DataFlavor
import java.awt.datatransfer.StringSelection
import java.awt.event.*
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import java.util.LinkedList
import javax.swing.SwingUtilities
import javax.swing.Timer
import kotlin.math.abs

class InputTextLine(
    private val panel: SlidePanel,
    x: Int,
    y: Int,
    width: Int,
    height: Int,
    val updateText: (text: String) -> Unit
) : ComponentOwn(),
    KeyListener,
    MouseListener, MouseWheelListener,
    MouseMotionListener {

    // Cursor blink timer (avoid background thread + repaint on EDT)
    private val blinkTimer = Timer(500) { panel.repaint(x, y, this.width, this.height) }

    init {
        this.x = x
        this.y = y
        this.height = height
        this.width = width
        blinkTimer.isRepeats = true
        blinkTimer.start()
    }

    fun dispose() {
        blinkTimer.stop()
    }

    private var rowHeight = 12

    private var image: BufferedImage? = null
    private var g2d: Graphics2D? = null
    private var maxCharBounds: Rectangle2D = Rectangle2D.Double(0.0, 0.0, 1.0, 1.0)

    private var selectedTextRange: IntRange? = null
    private var selectedText = ""

    private var mouseposX = 0
    private var mouseposY = 0
    private var mouseposXPressed = 0
    private var mouseposYPressed = 0
    private var mouseposXReleased = 0
    private var mouseposYReleased = 0

    private var lastKeyTimeStamp = System.currentTimeMillis()

    // History
    var textStack = LinkedList<String>()
    var textStackIndex = -1

    // IMPORTANT: make programmatic changes notify updateText too.
    // External code (e.g. LineItem) setting inputTextLine.text = "" will now trigger updateText().
    private var suppressNotify = false

    var text: String = ""
        set(value) {
            if (field == value) return
            field = value

            // keep cursor + selection consistent
            cursorIndex = cursorIndex.coerceIn(0..field.length)
            clearSelection()

            if (!suppressNotify) notifyUpdateText()
            panel.repaint(x, y, width, height)
        }

    var cursorIndex: Int = 0
        set(value) {
            field = value.coerceIn(0..text.length)
        }

    /** Set text from outside without risking duplicate updateText calls. */
    fun setTextProgrammatically(newText: String, cursorToEnd: Boolean = true, notify: Boolean = true) {
        suppressNotify = !notify
        try {
            text = newText
            if (cursorToEnd) cursorIndex = text.length
        } finally {
            suppressNotify = false
        }
    }

    private fun notifyUpdateText() {
        // ensure callback runs on EDT
        if (SwingUtilities.isEventDispatchThread()) {
            updateText(text)
        } else {
            SwingUtilities.invokeLater { updateText(text) }
        }
    }

    private fun clearSelection() {
        selectedText = ""
        selectedTextRange = null
    }

    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        if (image == null || this.width != width || this.height != height || this.x != x || this.y != y) {
            g2d?.dispose()
            image = BufferedImage(width.coerceAtLeast(1), height.coerceAtLeast(1), BufferedImage.TYPE_INT_RGB)
            g2d = image!!.createGraphics()
            this.height = height
            this.width = width
            this.x = x
            this.y = y
        }

        val g = g2d!!

        // background
        g.color = UiColors.background
        g.fillRect(0, 0, width, height)

        g.font = Font(Styles.normalFont, Font.PLAIN, rowHeight)
        maxCharBounds = g.fontMetrics.getMaxCharBounds(g)

        paintSelectedText(g)
        paintText(g)
        paintCursor(g)

        return image!!
    }

    override fun repaint(componentOwn: ComponentOwn) {
        // not used in your setup
    }

    private fun paintText(g: Graphics2D) {
        g.color = UiColors.defaultText
        g.drawString(text, 0, maxCharBounds.height.toInt())
    }

    private fun paintCursor(g: Graphics2D) {
        g.color = UiColors.defaultCursor
        val blinkOn = (System.currentTimeMillis() % 1000) > 500 || (System.currentTimeMillis() - lastKeyTimeStamp) < 500
        if (!blinkOn) return

        val safeIdx = cursorIndex.coerceIn(0..text.length)
        val cursorX = (g.fontMetrics.stringWidth(text.substring(0, safeIdx)) - 1).coerceAtLeast(0)

        g.fillRect(
            cursorX,
            g.fontMetrics.maxDescent,
            2,
            maxCharBounds.height.toInt()
        )
    }

    private fun paintSelectedText(g: Graphics2D) {
        val range = selectedTextRange ?: return
        if (text.isEmpty() || selectedText.isBlank()) return

        val start = range.first.coerceIn(0..text.length)
        val end = range.last.coerceIn(0..text.length)
        if (end <= start) return

        g.color = UiColors.selectionText
        val x = g.fontMetrics.stringWidth(text.substring(0, start))
        val w = g.fontMetrics.stringWidth(text.substring(start, end))
        g.fillRect(x, g.fontMetrics.maxDescent, w, maxCharBounds.height.toInt())
    }

    // ---------- Keyboard ----------

    override fun keyTyped(e: KeyEvent) {
        if (!Character.isISOControl(e.keyChar)) {
            insertCharAtCursorIndex(e.keyChar)
            cursorIndex(1)
            lastKeyTimeStamp = System.currentTimeMillis()
            // updateText is triggered by text setter
        }
    }

    override fun keyPressed(e: KeyEvent) {
        when (e.keyCode) {
            KeyEvent.VK_ESCAPE -> {
                // Clear query & notify
                setTextProgrammatically("", cursorToEnd = true, notify = true)
            }

            KeyEvent.VK_RIGHT -> {
                if ((e.isMetaDown && State.onMac) || (e.isAltDown && !State.onMac)) {
                    // history forward
                    if (textStack.isNotEmpty()) {
                        textStackIndex = (textStackIndex + 1).coerceIn(textStack.indices)
                        setTextProgrammatically(textStack[textStackIndex], cursorToEnd = true, notify = true)
                    }
                } else if (e.isShiftDown) {
                    // extend selection right
                    cursorIndex(1)
                    extendSelectionToCursor()
                } else {
                    cursorIndex(1)
                    if (selectedTextRange != null) {
                        cursorIndex = (selectedTextRange!!.last + 1).coerceIn(0..text.length)
                        clearSelection()
                    }
                }
            }

            KeyEvent.VK_LEFT -> {
                if ((e.isMetaDown && State.onMac) || (e.isAltDown && !State.onMac)) {
                    // history backward
                    if (textStack.isNotEmpty()) {
                        textStackIndex = (textStackIndex - 1).coerceIn(textStack.indices)
                        setTextProgrammatically(textStack[textStackIndex], cursorToEnd = true, notify = true)
                    }
                } else if (e.isShiftDown) {
                    // extend selection left
                    cursorIndex(-1)
                    extendSelectionToCursor()
                } else {
                    cursorIndex(-1)
                    if (selectedTextRange != null) {
                        cursorIndex = selectedTextRange!!.first.coerceIn(0..text.length)
                        clearSelection()
                    }
                }
            }

            KeyEvent.VK_ENTER -> {
                // history commit
                while (textStack.size > textStackIndex + 1 && textStack.isNotEmpty()) {
                    textStack.removeLast()
                }
                if (textStack.isEmpty() || textStack.last() != text) {
                    textStack.addLast(text)
                    textStackIndex = textStack.lastIndex
                }
            }

            KeyEvent.VK_BACK_SPACE -> {
                if (selectedText.isNotBlank() && selectedTextRange != null) {
                    val r = selectedTextRange!!
                    val start = r.first.coerceIn(0..text.length)
                    val endExclusive = (r.last + 1).coerceIn(0..text.length)
                    setTextProgrammatically(text.removeRange(start, endExclusive), cursorToEnd = false, notify = true)
                    cursorIndex = start
                    clearSelection()
                } else if (cursorIndex > 0) {
                    val idx = cursorIndex - 1
                    setTextProgrammatically(text.removeRange(idx, idx + 1), cursorToEnd = false, notify = true)
                    cursorIndex = idx
                }
            }

            KeyEvent.VK_C -> {
                if ((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) {
                    if (selectedText.isBlank()) return
                    val clipboard: Clipboard = Toolkit.getDefaultToolkit().systemClipboard
                    val stringSelection = StringSelection(selectedText)
                    clipboard.setContents(stringSelection, stringSelection)
                }
            }

            KeyEvent.VK_A -> {
                if ((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) {
                    selectedText = text
                    selectedTextRange = text.indices
                    cursorIndex = text.length
                    panel.repaint(x, y, width, height)
                }
            }

            KeyEvent.VK_V -> {
                if ((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) {
                    pasteFromClipboard()
                }
            }
        }

        lastKeyTimeStamp = System.currentTimeMillis()
        panel.repaint(x, y, width, height)
    }

    override fun keyReleased(e: KeyEvent) {
        panel.repaint(x, y, width, height)
    }

    private fun pasteFromClipboard() {
        val clipboard = Toolkit.getDefaultToolkit().systemClipboard
        val flavor = DataFlavor.stringFlavor
        if (!clipboard.isDataFlavorAvailable(flavor)) return

        val pasted = try {
            clipboard.getData(flavor) as String
        } catch (_: Exception) {
            return
        }

        if (selectedText.isNotBlank() && selectedTextRange != null) {
            val r = selectedTextRange!!
            val start = r.first.coerceIn(0..text.length)
            val endExclusive = (r.last + 1).coerceIn(0..text.length)
            val newText = text.substring(0, start) + pasted + text.substring(endExclusive)
            setTextProgrammatically(newText, cursorToEnd = false, notify = true)
            cursorIndex = start + pasted.length
            clearSelection()
        } else {
            // insert at cursor
            val start = cursorIndex.coerceIn(0..text.length)
            val newText = text.substring(0, start) + pasted + text.substring(start)
            setTextProgrammatically(newText, cursorToEnd = false, notify = true)
            cursorIndex = start + pasted.length
        }
    }

    private fun insertCharAtCursorIndex(keyChar: Char) {
        val idx = cursorIndex.coerceIn(0..text.length)
        setTextProgrammatically(
            text.substring(0, idx) + keyChar + text.substring(idx),
            cursorToEnd = false,
            notify = true
        )
    }

    private fun cursorIndex(delta: Int): Boolean {
        val before = cursorIndex
        cursorIndex = (cursorIndex + delta).coerceIn(0..text.length)
        return cursorIndex != before
    }

    private fun extendSelectionToCursor() {
        if (text.isEmpty()) return
        val idx = cursorIndex.coerceIn(0..text.length)

        val newRange = if (selectedTextRange == null) {
            // starting selection: anchor at previous cursor move
            val start = (idx - 1).coerceIn(0..text.length)
            start..idx
        } else {
            val r = selectedTextRange!!
            minOf(r.first, idx)..maxOf(r.last, idx)
        }

        selectedTextRange = newRange
        selectedText = text.substring(newRange.first.coerceIn(0..text.length), newRange.last.coerceIn(0..text.length))
    }

    // ---------- Mouse ----------

    override fun mouseClicked(e: MouseEvent) {
        if (e.clickCount == 1) {
            cursorIndex = getCharIndexFromMouse(text, mouseposX)
            cursorIndex(0)
            clearSelection()
        }

        if (e.clickCount == 2) {
            val idx = getCharIndexFromMouse(text, mouseposX)
            selectedTextRange = getWordRangeAtIndex(text, idx)
            selectedText = selectedTextRange?.let { text.substring(it) } ?: ""
            cursorIndex = (selectedTextRange?.last ?: idx).coerceIn(0..text.length)
        }

        if (e.clickCount == 3) {
            selectedText = text
            selectedTextRange = if (text.isNotEmpty()) text.indices else null
            cursorIndex = text.length
        }

        panel.repaint(x, y, width, height)
    }

    override fun mousePressed(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7
        mouseposXPressed = mouseposX
        mouseposYPressed = mouseposY
        mouseposXReleased = mouseposX
        mouseposYReleased = mouseposY
        clearSelection()
        panel.repaint(x, y, width, height)
    }

    override fun mouseReleased(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y
        mouseposXReleased = mouseposX
        mouseposYReleased = mouseposY

        val a = getCharIndexFromMouse(text, mouseposXPressed)
        val b = getCharIndexFromMouse(text, mouseposXReleased)
        val (start, end) = listOf(a, b).sorted()

        selectedTextRange = (start..end)
        selectedText = if (text.isNotEmpty()) text.substring(start.coerceIn(0..text.length), end.coerceIn(0..text.length)) else ""
        panel.repaint(x, y, width, height)
    }

    override fun mouseDragged(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7
        mouseposXReleased = mouseposX
        mouseposYReleased = mouseposY
        cursorIndex = getCharIndexFromMouse(text, mouseposX)
        panel.repaint(x, y, width, height)
    }

    override fun mouseMoved(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7
        panel.repaint(x, y, width, height)
    }

    override fun mouseEntered(e: MouseEvent) {
        mouseInside = true
    }

    override fun mouseExited(e: MouseEvent) {
        mouseInside = false
    }

    override fun mouseWheelMoved(e: MouseWheelEvent) {
        if ((e.isControlDown && !State.onMac) || (e.isMetaDown && State.onMac)) {
            rowHeight = (rowHeight + e.wheelRotation).coerceIn(1..100)
            mouseposXPressed = 0
            mouseposYPressed = 0
            mouseposXReleased = 0
            mouseposYReleased = 0
            panel.repaint(x, y, width, height)
        }
    }

    private fun isDelimiter(c: Char): Boolean = c == ' ' || c == '"'

    fun getWordRangeAtIndex(input: String, charIndex: Int): IntRange? {
        if (input.isEmpty()) return null
        val idx = charIndex.coerceIn(0..input.lastIndex)
        if (isDelimiter(input[idx])) return null

        var start = idx
        var end = idx

        while (start > 0 && !isDelimiter(input[start - 1])) start--
        while (end < input.lastIndex && !isDelimiter(input[end + 1])) end++

        return start..end
    }

    private fun getCharIndexFromMouse(string: String, x: Int): Int {
        val g = g2d ?: return 0
        if (string.isEmpty()) return 0
        if (x <= 0) return 0

        // O(n) scan; avoids building strings each iteration
        val fm = g.fontMetrics
        var w = 0
        for (i in string.indices) {
            w += fm.charWidth(string[i])
            if (w > x) return i
        }
        return string.length
    }
}