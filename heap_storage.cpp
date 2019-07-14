#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <iostream>
#include <string>
#include <cassert>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"
#include "heap_storage.h"
using namespace std;
using namespace hsql;

typedef u_int16_t u16;


/*
//	Slottedpage structure is used to organize records within a block, 
//	Records are allocated contiguously, also the free same in the block
//	are contiguous between the array of headers and the first record.
*/

//	Constructor for the Slottedpage class.
//
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new)
{
	if (is_new)
	{
		this->num_records = 0;
		this->end_free = DbBlock::BLOCK_SZ - 1;
		put_header(); 
	}
	else
	{
		get_header(this->num_records, this->end_free);
	}
}

// Get a 2-byte integer at given offset in block.
//
u16 SlottedPage::get_n(u16 offset)
{
	return *(u16*)(this->address(offset));
}

// Put a 2-byte integer at given offset in block.
//
void SlottedPage::put_n(u16 offset, u16 n)
{
	*(u16*)(this->address(offset)) = n;
}

// Make a void* pointer for a given offset into the data block.
//
void* SlottedPage::address(u16 offset)
{
	return (void*)((char*)this->block.get_data() + offset);
}

// Get the size and offset for given id. For id 0 return the block header.
//
void SlottedPage::get_header(u_int16_t &record_size, u_int16_t &location, RecordID id)
{
	record_size = get_n(4 * id);
	location = get_n(4 * id + 2);
}
// Put the size and offset for given id. For id 0 return the block header.
//
void SlottedPage::put_header(RecordID id, u_int16_t record_size, u_int16_t location)
{
	if (id == 0)
	{
		record_size = this-> num_records;
		location = this-> end_free;
	}
	put_n(4 * id, record_size);
	put_n(4 * id + 2, location);
}

// Add a new rocord to the block and return the id of the new record.
//
RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError)
{
	if (!has_room(data->get_size()))
		throw DbBlockNoRoomError("not enough room for new record");
	u16 id = ++this->num_records;
	u16 size = (u16)data->get_size();
	this->end_free -= size;
	u16 loc = this->end_free + 1;
	put_header();
	put_header(id, size, loc);
	memcpy(this->address(loc), data->get_data(), size);
	return id;
}

//Get record from the block for the given record_id, if not found return NULL.
//
Dbt* SlottedPage::get(RecordID record_id)
{
	u16 record_size;
	u16 location;
	get_header(record_size, location, record_id);
	if (location == 0)
		return nullptr;
	
	Dbt* returnRecord = new Dbt(this->address(location), record_size);
	return returnRecord;
}

/*
	Delete record from the block for given record_id and change the record_size 
	and location of the record_id to 0 
*/
void SlottedPage::del(RecordID record_id)
{
	u16 record_size;
	u16 location;
	get_header(record_size, location, record_id);
	put_header(record_size, 0, 0);
	slide(location, location + record_size);
}

/*
	Slide is used to shift data. 
	If start < end, remove data from the offset start up by sliding data from the
	left of start to its right.
	If start > end, get space for the extra data from end to start by sliding data 
	from the left of the start to its right.
*/
void SlottedPage::slide(u16 start, u16 end)
{
	u16 shift = end - start;
	if (shift == 0)
		return;
	
	u16 size = end - (this->end_free + 1 + shift);
	memcpy(this->address(this->end_free + 1 + shift), this->address(this->end_free + 1), size);
	RecordIDs* arr_recordIds = ids();
	for (auto const& record_id : *arr_recordIds)
	{
		RecordID id = record_id;
		u16 record_size;
		u16 location;
		get_header(record_size, location, id);
		if (location <= start)
		{
			location += shift;
			put_header(id, record_size, location);
		}
		this->end_free += shift;
		put_header();
	}

}

// Replace the record with given data, if the space is not enough raise an error.
//
void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError)
{
	u16 record_size;
	u16 location;
	get_header(record_size, location, record_id);
	u16 new_size = (u16)data.get_size();
	if (new_size > record_size)
	{
		u16 extra = new_size - record_size;
		if (!has_room(extra))
		{
			throw DbBlockNoRoomError("Not enough room in block");
		}
		slide(location, location - extra);
		memcpy(this->address(location - extra), data.get_data(), new_size);
	}
	else
	{
		memcpy(this->address(location), data.get_data(), new_size);
		slide(location + new_size, location + record_size);
		u16 record_size;
		u16 location;
		get_header(record_size, location, record_id);
		put_header(record_id, new_size, location);
	}
}

// To get sequence of all non-deleted record ids.
//
RecordIDs* SlottedPage::ids(void)
{
	//remaining
	u16 record_size = 0;
	u16 location = 0;
	u16 recordNum = this->num_records;

	RecordIDs* recordIDVector = new RecordIDs();
	for (int i = 1; i <= recordNum; i++)
	{
		get_header(record_size, location, i);
		if (location != 0)
		{
			recordIDVector->push_back(i);
		}
	}
	return recordIDVector;
}

// Calculates if it has engough sapce to store the record with given size. 
//
bool SlottedPage::has_room(u16 size)
{
	u16 available = this->end_free - (this->num_records + 2) * 4;
	if (size <= available)
	{
		return true;
	}
	else
		return false;
}

/*
	Heap file organization. Built on top of Berkeley DB RecNo file. There is one of
	our database blocks for each Berkeley DB record in the RecNo file. In this way
	we are using Berkeley DB for buffer management and file management.
    Uses SlottedPage for storing records within blocks.
*/
//
void HeapFile::create(void)
{
	this->db_open(DB_CREATE | DB_EXCL);
	DbBlock *block = this->get_new();
	this->put(block);
	delete block;
}

void HeapFile::drop(void)
{
	this->close();
	remove(this->dbfilename.c_str());
}

void HeapFile::open(void)
{
	this->db_open();
}

void HeapFile::close(void)
{
	this->db.close(0);
	this->closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
//
SlottedPage* HeapFile::get_new(void)
{
	char block[DbBlock::BLOCK_SZ];
	std::memset(block, 0, sizeof(block));
	Dbt data(block, sizeof(block));
	int block_id = ++this->last;
	Dbt key(&block_id, sizeof(block_id));
	// write out an empty block and read it back in so Berkeley DB is managing the memory
	SlottedPage* page = new SlottedPage(data, this->last, true);
	this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
	this->db.get(nullptr, &key, &data, 0);
	return page;
}

SlottedPage* HeapFile::get(BlockID block_id)
{
	char block[DbBlock::BLOCK_SZ];
	Dbt data(block, sizeof(block));
	Dbt key(&block_id, sizeof(block_id));
	this->db.get(nullptr, &key, &data, 0);
	SlottedPage* page = new SlottedPage(data, block_id, false);
	return page;

}

void HeapFile::put(DbBlock* block)
{
	BlockID blockID = block->get_block_id();
	void* blockData = block->get_data();
	Dbt key(&blockID, sizeof(blockID));
	Dbt dbtData(blockData, DbBlock::BLOCK_SZ);
	this->db.put(nullptr, &key, &dbtData, 0);
}

BlockIDs* HeapFile::block_ids()
{
	BlockIDs* blockIDs = new BlockIDs;
	for (BlockID i = 1; i <= this->last; i++)
	{
		blockIDs->push_back(i);
	}
	return blockIDs;

}

void HeapFile::db_open(uint flags)
{
	if (!this->closed)
	{
		return;
	}
	this->db.set_re_len(DbBlock::BLOCK_SZ);
	const char* filepath = nullptr;
	_DB_ENV->get_home(&filepath);
	string path = "../";
	this->dbfilename = path + filepath + '/' + this->name + ".db";
	this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0);
	DB_BTREE_STAT db_stats;
	this->db.stat(nullptr, &db_stats, DB_FAST_STAT);
	this->last = db_stats.bt_ndata;
	this->closed = false;	
}

/*
	Heap storage engine
*/
// Constructor for class HeapTable.
//
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) 
	: DbRelation(table_name, column_names, column_attributes),
	file(table_name)
{
}

// Creates a table.
//
void HeapTable::create()
{
	this->file.create();
}

// Opens existing table with permission to insert, update, delete, select, project.
//
void HeapTable::open()
{
	this->file.open();
}

//Closes the table with disablong operations like insert, delete, select, project.
//
void HeapTable::close()
{
	this->file.close();
}

//Create a table if it does not exists.
//
void HeapTable::create_if_not_exists()
{
	try
	{
		this->open();
	}
	catch (const std::exception& e)
	{
		this->create();
	}
}

// Drop table.
//
void HeapTable::drop()
{
	this->file.drop();
}

// Insert row into the table and return the handle of the inserted row.
//
Handle HeapTable::insert(const ValueDict* row)
{
	this->open();
	return this->append(this->validate(row));
}

// Expect new values with column name keys.
//
void HeapTable::update(const Handle handle, const ValueDict* new_values)
{
	ValueDict* row = project(handle);
	for (auto const& key : *new_values)
	{
		ValueDict::const_iterator column = new_values->find(key.first);
		Value value = column->second;
		row->insert(std::pair<Identifier, Value>(key.first, value));
	}
	ValueDict* full_row = this->validate(row);
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
	DbBlock* block = this->file.get(block_id);
	block->put(record_id, *(this->marshal(full_row))); //this method is of class HeapFile
	this->file.put(block);
}

// Delete from table with specified handle.
//
void HeapTable::del(const Handle handle)
{
	this->open();
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
	DbBlock* block = this->file.get(block_id);
	block->del(record_id);
	this->file.put(block);
}

// Select without a where clause.
//
Handles* HeapTable::select() 
{
	return select(nullptr);
}

// Select with a where clause, returns a list of handles for qualifying rows.
//
Handles* HeapTable::select(const ValueDict* whr)
{
	Handles* handles = new Handles();
	BlockIDs* block_ids = file.block_ids();
	for (auto const& block_id : *block_ids) 
	{
		SlottedPage* block = file.get(block_id);
		RecordIDs* record_ids = block->ids();
		for (auto const& record_id : *record_ids)
		{
			//if(whr == nullptr 
			//	|| this->selected(Handle(block_id, record_id), whr))
			handles->push_back(Handle(block_id, record_id));
		}
		delete record_ids;
		delete block;
	}
	delete block_ids;
	return handles;
}

/*
bool HeapTable::selected(const Handle handle, const ValueDict* whr) {
	ValueDict* row = this->project(handle, whr);
	for (auto const& col_names : *whr) {
		if (row->find(col_names.first) == row->end())
			return false;
	}
	return true;
}
*/

// Check if row can be accepted to insert. Raise an error if not.
//
ValueDict* HeapTable::validate(const ValueDict* row)
{
	ValueDict* full_row = new ValueDict();
	for (auto const& col_name : this->column_names)
	{
		if (row->find(col_name) == row->end())
		{
			throw DbRelationError("Cannot insert the row as column name is not acceptable");
		}
		else
		{
			ValueDict::const_iterator column = row->find(col_name);
			Value value = column->second;
			full_row->insert(std::pair<Identifier, Value>(col_name, value));
		}
	}

	return full_row;
}


// Return the bits to go into the file.
//
Dbt* HeapTable::marshal(const ValueDict* row)
{
	char* bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
	uint offset = 0;
	uint col_num = 0;
	for (auto const& column_name : this->column_names)
	{
		ColumnAttribute ca = this->column_attributes[col_num++];
		ValueDict::const_iterator column = row->find(column_name);
		Value value = column->second;
		if (ca.get_data_type() == ColumnAttribute::DataType::INT)
		{
			*(int32_t*)(bytes + offset) = value.n;
			offset += sizeof(int32_t);
		}
		else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) 
		{
			uint size = value.s.length();
			*(u16*)(bytes + offset) = size;
			offset += sizeof(u16);
			memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
			offset += size;
		}
		else
		{
			throw DbRelationError("Only know how to marshal INT and TEXT");
		}
	}
	char* right_size_bytes = new char[offset];
	memcpy(right_size_bytes, bytes, offset);
	delete[] bytes;
	Dbt* data = new Dbt(right_size_bytes, offset);
	return data;
}

ValueDict* HeapTable::unmarshal(Dbt* data)
{
	ValueDict* row = new ValueDict();
	uint offset = 0;
	uint col_num = 0;
	char* bytes = (char *)data->get_data();
	for (auto const& column_name : this->column_names)
	{
		ColumnAttribute ca = this->column_attributes[col_num++];
		if (ca.get_data_type() == ColumnAttribute::DataType::INT)
		{
			row->insert(std::pair<Identifier, Value>(column_name, Value(*(int32_t*)(bytes + offset))));
			offset += sizeof(int32_t);
		}
		else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
		{
			u16 size = *(u16*)(bytes + offset);
			offset += sizeof(u16);
			char strBuffer[DbBlock::BLOCK_SZ];
			memcpy(strBuffer, bytes + offset, size); // assume ascii for now
			strBuffer[size] = '\0';
			offset += size;
			row->insert(std::pair<Identifier, Value>(column_name, Value(strBuffer)));
		}
		else
		{
			throw DbRelationError("Only know how to unmarshal INT and TEXT");
		}
	}

	return row;
}

// Appends a record to the file, assuming the row is fully fleshed-out.
//
Handle HeapTable::append(const ValueDict* row)
{
	Dbt* data = this->marshal(row);
	DbBlock* block = this->file.get(this->file.getLast());
	RecordID record_id;
	try 
	{
		record_id = block->add(data);
	}
	catch (DbBlockNoRoomError& excep)
	{
		block = this->file.get_new();
		record_id = block->add(data);
	}
	this->file.put(block);
	delete block;
	delete[](char*)data->get_data();
	delete data;
	return Handle(this->file.getLast(), record_id);
}

// Return a sequence of values for given handle.
//
ValueDict* HeapTable::project(Handle handle)
{
	return this->project(handle, &this->column_names);
}

// Return a sequence of values for handle given to column_names.
//
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names)
{
	ValueDict* returnRow = new ValueDict();
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;

	DbBlock* block = this->file.get(block_id);
	Dbt* data = block->get(record_id);

	returnRow = this->unmarshal(data);

	if (column_names == nullptr || column_names->empty())
	{
		return returnRow;
	}
	ValueDict* row = new ValueDict();
	for (auto const& column_name : *column_names) {
		if (returnRow->find(column_name) == returnRow->end()) {
			throw DbRelationError("The column '" + column_name + "'does not exist in the table.");
		}
		row->insert(std::pair<Identifier, Value>(column_name, (*returnRow)[column_name]));
	}
	delete returnRow;
	delete block;
	delete data;
	return row;
}
