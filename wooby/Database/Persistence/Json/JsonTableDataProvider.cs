using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby.Database.Persistence.Json
{
    public class JsonTableDataProvider : ITableDataProvider
    {
        public long Delete(long rowid)
        {
            throw new NotImplementedException();
        }

        public void Initialize(Context context, TableMeta meta)
        {
            throw new NotImplementedException();
        }

        public long Insert(Dictionary<int, ColumnValue> values)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<ColumnValue> Seek(long RowId)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<ColumnValue> SeekNext(ref long RowId)
        {
            throw new NotImplementedException();
        }

        public void Update(long rowid, Dictionary<int, ColumnValue> columns)
        {
            throw new NotImplementedException();
        }
    }
}
