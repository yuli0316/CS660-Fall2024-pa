#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>



using namespace db;

// 构造函数：初始化HeapPage对象
HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
  size_t tupleSize = td.length();  // 获取每个元组的大小
  size_t pageSize = DEFAULT_PAGE_SIZE;  // 假设页面大小是预定义的常量（比如 4096 字节）

  // 计算页面中可以容纳的元组数目
  capacity = (pageSize * 8) / (tupleSize * 8 + 1);  // 使用给定的公式计算容量

  // 初始化header和data指针
  header = reinterpret_cast<uint8_t*>(page.data());  // header 指向页面缓冲区的起始位置

  // data 指向实际的元组数据，位于 (页面大小 - (元组大小 * 可容纳的元组数目)) 的位置
  data = reinterpret_cast<uint8_t*>(page.data() + pageSize - (capacity * tupleSize));
}

// 返回页面的第一个非空槽位的索引
size_t HeapPage::begin() const {
  for (size_t i = 0; i < capacity; ++i) {
    if (!empty(i)) {
      return i;  // 找到第一个非空槽位
    }
  }
  return end();  // 所有槽位为空时返回 end()
}

// 返回页面的容量，用作页面的结束标志
size_t HeapPage::end() const {
  return capacity;  // 页面容量的末尾
}

// 插入元组，如果成功返回true，如果页面已满则返回false
bool HeapPage::insertTuple(const Tuple &t) {
  for (size_t i = 0; i < capacity; ++i) {
    if (empty(i)) {  // 找到一个空的槽位
      size_t offset = i * td.length();  // 计算元组的偏移量
      td.serialize(data + offset, t);  // 将元组序列化到页面的指定位置

      // 设置位图，标记槽位为已使用
      size_t byteIndex = i / 8;
      size_t bitIndex = 7 - i % 8;
      header[byteIndex] |= (1 << bitIndex);  // 标记槽位为占用
      return true;
    }
  }
  return false;  // 页面已满，插入失败
}

// 删除指定槽位的元组
void HeapPage::deleteTuple(size_t slot) {
  // 检查槽位是否超出页面容量
  if (slot >= capacity) {
    throw std::out_of_range("Slot out of range");
  }

  // 检查槽位是否已经为空
  if (empty(slot)) {
    throw std::runtime_error("Slot is already empty");
  }
  size_t byteIndex = slot / 8;
  size_t bitIndex = 7 - slot % 8;
  // 3. 生成位掩码，目标位为0，其他位为1
  uint8_t mask = ~(1 << bitIndex);
  // 4. 使用掩码将目标位清零，标记槽位为空
  header[byteIndex] &= mask;
  // 5. 可选：清除该槽位的数据，将其置为全零
  size_t offset = slot * td.length();  // 计算元组数据的偏移量
}


// 获取指定槽位的元组
Tuple HeapPage::getTuple(size_t slot) const {
  if (slot >= capacity || empty(slot)) {
    throw std::runtime_error("Slot is empty or out of range");
  }
  size_t offset = slot * td.length();  // 计算元组的偏移量
  return td.deserialize(data + offset);  // 从页面中反序列化元组
}

void HeapPage::next(size_t &slot) const {
  for (size_t i = slot + 1; i < capacity; ++i) {
    if (!empty(i)) {  // 找到下一个非空槽位
      slot = i;
      return;
    }
  }
  slot = end();  // 如果没有更多非空槽位，则设置为页面的末尾
}

bool HeapPage::empty(size_t slot) const {
  if (slot >= capacity) {
    throw std::out_of_range("Slot out of range");
  }

  // 逐字节读取 header，逐位检查槽位状态
  size_t byteIndex = slot / 8;
  size_t bitIndex = 7 - slot % 8;

  uint8_t byte = header[byteIndex];  // 读取该字节
  return !(byte & (1 << bitIndex));  // 检查对应位是否为0，表示槽位为空
}


