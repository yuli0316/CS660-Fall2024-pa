#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>



using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
  size_t tupleSize = td.length();
  size_t pageSize = DEFAULT_PAGE_SIZE;

  capacity = (pageSize * 8) / (tupleSize * 8 + 1);
  header = reinterpret_cast<uint8_t*>(page.data());

  data = reinterpret_cast<uint8_t*>(page.data() + pageSize - (capacity * tupleSize));
}

size_t HeapPage::begin() const {
  for (size_t i = 0; i < capacity; ++i) {
    if (!empty(i)) {
      return i;
    }
  }
  return end();
}

size_t HeapPage::end() const {
  return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
  for (size_t i = 0; i < capacity; ++i) {
    if (empty(i)) {
      size_t offset = i * td.length();
      td.serialize(data + offset, t);

      size_t byteIndex = i / 8;
      size_t bitIndex = 7 - i % 8;
      header[byteIndex] |= (1 << bitIndex);
      return true;
    }
  }
  return false;
}

void HeapPage::deleteTuple(size_t slot) {
  if (slot >= capacity) {
    throw std::out_of_range("Slot out of range");
  }

  if (empty(slot)) {
    throw std::runtime_error("Slot is already empty");
  }
  size_t byteIndex = slot / 8;
  size_t bitIndex = 7 - slot % 8;
  uint8_t mask = ~(1 << bitIndex);
  header[byteIndex] &= mask;
  size_t offset = slot * td.length();
}


Tuple HeapPage::getTuple(size_t slot) const {
  if (slot >= capacity || empty(slot)) {
    throw std::runtime_error("Slot is empty or out of range");
  }
  size_t offset = slot * td.length();
  return td.deserialize(data + offset);
}

void HeapPage::next(size_t &slot) const {
  for (size_t i = slot + 1; i < capacity; ++i) {
    if (!empty(i)) {
      slot = i;
      return;
    }
  }
  slot = end();
}

bool HeapPage::empty(size_t slot) const {
  if (slot >= capacity) {
    throw std::out_of_range("Slot out of range");
  }

  size_t byteIndex = slot / 8;
  size_t bitIndex = 7 - slot % 8;

  uint8_t byte = header[byteIndex];
  return !(byte & (1 << bitIndex));
}


