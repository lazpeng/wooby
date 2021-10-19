using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby.Database.Persistence
{
    public interface ITableDataProvider
    {
        // Reads and returns a row with the given rowid
        IEnumerable<ColumnValue> Seek(long RowId);
        // Reads the next row and updates the rowid argument to the current row's id
        IEnumerable<ColumnValue> SeekNext(ref long RowId);
        // Deletes a row, returning the previous rowId (row before the one deleted) or long.MinValue
        long Delete(long rowid);
        // Creates a new row with the given values using a dictionary of (ColumnIndex, ColumnValue) and returns its rowid
        long Insert(Dictionary<int, ColumnValue> values);
        // Updates a row with the given id using the given dictionary of (ColumnIndex, ColumnValue)
        void Update(long rowid, Dictionary<int, ColumnValue> columns);
        // Initialize the data provider
        void Initialize(Context context, TableMeta meta);
    }
}
