package widgets

import ComponentOwn
import SlidePanel
import State
import app.*
import round
import util.Styles
import util.UiColors
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
  private val filteredItems: MutableList<Item> = mutableListOf()
  private var filterText = ""
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
    g2d.color = UiColors.background
    g2d.fillRect(0, 0, width, height)
    g2d.font = Font(Styles.normalFont, Font.PLAIN, rowHeight)
    maxCharBounds = g2d.fontMetrics.getMaxCharBounds(g2d)

    g2d.color = UiColors.magenta
    drawSelectedLine()
    paint()
    return image
  }

  override fun repaint(componentOwn: ComponentOwn) {
    TODO("Not yet implemented")
  }

  private fun paint() {
    g2d.color = UiColors.defaultText
    filteredItems.mapIndexed { index, it ->
      g2d.color = if (it.selected) UiColors.green else UiColors.defaultText
      g2d.drawString(it.name + " " + it.version, 0, maxCharBounds.height.toInt() * (index + 1))
    }
  }

  private fun drawSelectedLine() {
    g2d.color = UiColors.selectionLine
    val height = maxCharBounds.height.toInt() + g2d.fontMetrics.maxDescent
    g2d.fillRect(0, maxCharBounds.height.toInt() * selectedLineIndex, width, height)
  }

  override fun keyTyped(e: KeyEvent) {

  }

  override fun keyPressed(e: KeyEvent) {
    when {
      ((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) && e.keyCode == KeyEvent.VK_A -> {
        filteredItems.forEach { item ->
          item.selected = !item.selected
          Channels.podsChannel.put(if (item.selected) ListenToPod(podName = item.name) else UnListenToPod(podName = item.name))
        }
        panel.repaint()
      }
      ((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) && e.keyCode == KeyEvent.VK_P -> {
        Channels.podsChannel.put(UnListenToPods)
        val listPods = ListPods()
        Channels.podsChannel.put(listPods)
        items.clear()
        items.addAll(listPods.result.get().map {
          Item(name = it.name, version = it.version, creationDate = it.creationTimestamp, selected = false)
        })
        applyFilter()
        panel.repaint()
      }
      e.keyCode == KeyEvent.VK_DOWN -> {
        selectedLineIndex = (selectedLineIndex + 1).coerceIn(0 until filteredItems.size)
        panel.repaint()
      }
      e.keyCode == KeyEvent.VK_UP -> {
        selectedLineIndex = (selectedLineIndex - 1).coerceIn(0 until filteredItems.size)
        panel.repaint()
      }
      e.keyCode == KeyEvent.VK_ENTER -> {
        val item = filteredItems[selectedLineIndex]
        item.selected = !item.selected
        Channels.podsChannel.put(if (item.selected) ListenToPod(podName = item.name) else UnListenToPod(podName = item.name))
        panel.repaint()
      }
      e.keyCode in 'A'.code..'Z'.code || e.keyCode in 'a'.code..'z'.code || e.keyCode in '0'.code..'9'.code -> {
        filterText += e.keyChar
        applyFilter()
        panel.repaint()
      }
      e.keyCode == KeyEvent.VK_BACK_SPACE -> {
        if (filterText.isNotEmpty()) {
          filterText = filterText.dropLast(1)
          applyFilter()
          panel.repaint()
        }
      }
      e.keyCode == KeyEvent.VK_P && State.onMac && e.isMetaDown -> {
        panel.repaint()
      }
    }
  }

  private fun applyFilter() {
    filteredItems.clear()
    if (filterText.isEmpty()) {
      filteredItems.addAll(items)
    } else {
      filteredItems.addAll(items.filter { it.name.contains(filterText, ignoreCase = true) })
    }
    // You might want to reset the selected index if needed
    selectedLineIndex = 0
  }

  override fun keyReleased(e: KeyEvent) {
    panel.repaint()
  }


  override fun mouseClicked(e: MouseEvent) {

  }


  override fun mousePressed(e: MouseEvent) {
    mouseposX = e.x - x
    mouseposY = e.y - y  // Removed the -7 offset that was causing issues
    mouseposXPressed = mouseposX
    mouseposYPressed = mouseposY
    mouseposXReleased = mouseposX
    mouseposYReleased = mouseposY
    selectedText = ""
    
    // Handle click-to-select functionality
    val clickedLineIndex = (mouseposY / maxCharBounds.height.toInt()).coerceAtLeast(0) - 1
    if (clickedLineIndex >= 0 && clickedLineIndex < filteredItems.size) {
      selectedLineIndex = clickedLineIndex
      val item = filteredItems[selectedLineIndex]
      item.selected = !item.selected
      Channels.podsChannel.put(if (item.selected) ListenToPod(podName = item.name) else UnListenToPod(podName = item.name))
    }
    
    panel.repaint()
  }

  override fun mouseReleased(e: MouseEvent) {
    mouseposX = e.x - x
    mouseposY = e.y - y  // Consistent offset removal
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
    mouseposY = e.y - y  // Consistent offset removal
    mouseposXReleased = mouseposX
    mouseposYReleased = mouseposY
    cursorIndex = getCharIndexFromMouse(text, mouseposX)
    panel.repaint()
  }

  override fun mouseMoved(e: MouseEvent) {
    mouseposX = e.x - x
    mouseposY = e.y - y  // Consistent offset removal

    panel.repaint()
  }

  enum class PodSelectState {
    nameSpace, podName
  }

  data class Item(
    val name: String,
    val version: String = "",
    val creationDate: String,
    var selected: Boolean
  )
}
