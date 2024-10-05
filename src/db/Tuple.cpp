#include <cstring>
#include <db/Tuple.hpp>
#include <stdexcept>

using namespace db;

Tuple::Tuple(const std::vector<field_t> &fields) : fields(fields) {}

type_t Tuple::field_type(size_t i) const {
  const field_t &field = fields.at(i);
  if (std::holds_alternative<int>(field)) {
    return type_t::INT;
  }
  if (std::holds_alternative<double>(field)) {
    return type_t::DOUBLE;
  }
  if (std::holds_alternative<std::string>(field)) {
    return type_t::CHAR;
  }
  throw std::logic_error("Unknown field type");
}

size_t Tuple::size() const { return fields.size(); }

const field_t &Tuple::get_field(size_t i) const { return fields.at(i); }

TupleDesc::TupleDesc(const std::vector<type_t> &types, const std::vector<std::string> &names) {
  if (types.size() != names.size()) {
    throw std::invalid_argument("Types and names size must match");
  }


  for (size_t i = 0; i < names.size(); ++i) {
    for (size_t j = i + 1; j < names.size(); ++j) {
      if (names[i] == names[j]) {
        throw std::logic_error("Field names must be unique");
      }
    }
  }


  this->types = types;
  this->names = names;
}



bool TupleDesc::compatible(const Tuple &tuple) const {
  if (types.empty() || names.empty()) {
    throw std::runtime_error("not implemented");
  }

  // 检查 tuple 的字段数是否和 TupleDesc 中的字段数匹配
  if (tuple.size() != types.size()) {
    return false;
  }

  for (size_t i = 0; i < types.size(); ++i) {
    if (tuple.field_type(i) != types[i]) {
      return false;
    }
  }

  return true;
}

size_t TupleDesc::index_of(const std::string &name) const {
  // TODO pa2: implement
  auto it = std::find(names.begin(), names.end(), name);
  if (it != names.end()) {
    return std::distance(names.begin(), it);
  }
  throw std::invalid_argument("Field name not found");
}

size_t TupleDesc::offset_of(const size_t &index) const {
  if (index >= types.size()) {
    throw std::out_of_range("Index out of range");
  }

  size_t offset = 0;
  for (size_t i = 0; i < index; ++i) {
    if (types[i] == type_t::INT) {
      offset += db::INT_SIZE;
    } else if (types[i] == type_t::DOUBLE) {
      offset += db::DOUBLE_SIZE;
    } else if (types[i] == type_t::CHAR) {
      offset += db::CHAR_SIZE;
    }
  }
  return offset;
}


size_t TupleDesc::length() const {
  // TODO pa2: implement
  size_t totalLength = 0;
  for (const auto &type : types) {
    if (type == type_t::INT) {
      totalLength += sizeof(int);
    } else if (type == type_t::DOUBLE) {
      totalLength += sizeof(double);
    } else if (type == type_t::CHAR) {
      totalLength += sizeof(char) * 64; // 假设 CHAR(64) 长度固定
    }
  }
  return totalLength;
}

size_t TupleDesc::size() const {
  // TODO pa2: implement
  return types.size();
}

Tuple TupleDesc::deserialize(const uint8_t *data) const {
  // TODO pa2: implement
  std::vector<field_t> fields;
  size_t offset = 0;
  for (const auto &type : types) {
    if (type == type_t::INT) {
      int value;
      std::memcpy(&value, data + offset, sizeof(int));
      fields.emplace_back(value);
      offset += sizeof(int);
    } else if (type == type_t::DOUBLE) {
      double value;
      std::memcpy(&value, data + offset, sizeof(double));
      fields.emplace_back(value);
      offset += sizeof(double);
    } else if (type == type_t::CHAR) {
      char value[64]; // 假设 CHAR(64)
      std::memcpy(value, data + offset, sizeof(char) * 64);
      fields.emplace_back(std::string(value));
      offset += sizeof(char) * 64;
    }
  }
  return Tuple(fields);
}

void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
  // TODO pa2: implement
  size_t offset = 0;
  for (size_t i = 0; i < types.size(); ++i) {
    const field_t &field = t.get_field(i);
    if (types[i] == type_t::INT) {
      int value = std::get<int>(field);
      std::memcpy(data + offset, &value, sizeof(int));
      offset += sizeof(int);
    } else if (types[i] == type_t::DOUBLE) {
      double value = std::get<double>(field);
      std::memcpy(data + offset, &value, sizeof(double));
      offset += sizeof(double);
    } else if (types[i] == type_t::CHAR) {
      const std::string &value = std::get<std::string>(field);
      std::memcpy(data + offset, value.c_str(), sizeof(char) * 64); // 假设 CHAR(64)
      offset += sizeof(char) * 64;
    }
  }
}

db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
  // TODO pa2: implement
  std::vector<type_t> mergedTypes = td1.types;
  std::vector<std::string> mergedNames = td1.names;
  mergedTypes.insert(mergedTypes.end(), td2.types.begin(), td2.types.end());
  mergedNames.insert(mergedNames.end(), td2.names.begin(), td2.names.end());
  return TupleDesc(mergedTypes, mergedNames);
}
