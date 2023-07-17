using System.Collections.Generic;

namespace wooby.Database.Persistence;

public interface ITableDataProvider
{
    // Reads and returns a row with the given rowid
    IEnumerable<BaseValue> Seek(long rowId);
    // Reads the next row and updates the rowid argument to the current row's id
    IEnumerable<BaseValue> SeekNext(ref long rowId);
    // Deletes a row, returning the previous rowId (row before the one deleted) or long.MinValue
    long Delete(long rowid);
    // Creates a new row with the given values using a dictionary of (ColumnIndex, ColumnValue) and returns its rowid
    long Insert(Dictionary<int, BaseValue> values);
    // Updates a row with the given id using the given dictionary of (ColumnIndex, ColumnValue)
    void Update(long rowid, Dictionary<int, BaseValue> columns);
    // Initialize the data provider
    void Initialize(Context context, TableMeta meta);
}