using System;
using System.Collections.Generic;
using System.Linq;

namespace wooby.Database.Persistence.Json
{
    public class JsonTableDataProvider : ITableDataProvider
    {
        private JsonCustomData PrivateData;
        private Dictionary<long, JsonTableRow> TableData;
        private TableMeta Meta;

        public long Delete(long rowid)
        {
            var index = TableData.Keys.ToList().IndexOf(rowid);
            TableData.Remove(rowid);

            if (TableData.Count == 0 || index >= TableData.Count)
            {
                return long.MinValue;
            } else
            {
                return TableData.Keys.ToList()[index];
            }
        }

        public void Initialize(Context context, TableMeta meta)
        {
            if (context.CustomSourceData != null && context.CustomSourceData is JsonCustomData json)
            {
                PrivateData = json;
                Meta = meta;
                if (PrivateData.Data.TryGetValue(meta.Id, out Dictionary<long, JsonTableRow> rows))
                {
                    TableData = rows;
                } else
                {
                    TableData = new Dictionary<long, JsonTableRow>();
                    PrivateData.Data.Add(meta.Id, TableData);
                }
            } else
            {
                throw new Exception("Failure to initialize JsonTableDataProvider: Custom data is not available");
            }
        }

        public long Insert(Dictionary<int, BaseValue> values)
        {
            var row = new JsonTableRow { RowId = PrivateData.NextRowId++, Columns = new List<BaseValue>(Meta.Columns.Count) };

            for (int idx = 0; idx < Meta.Columns.Count; ++idx)
            {
                if (values.TryGetValue(idx, out BaseValue v))
                {
                    row.Columns.Add(v);
                }
                else
                {
                    row.Columns.Add(new NullValue());
                }
            }

            TableData.Add(row.RowId, row);

            return row.RowId;
        }

        public IEnumerable<BaseValue> Seek(long RowId)
        {
            if (TableData.TryGetValue(RowId, out JsonTableRow row))
            {
                return row.Columns;
            } else
            {
                return null;
            }
        }

        public IEnumerable<BaseValue> SeekNext(ref long RowId)
        {
            bool found = RowId == long.MinValue;

            foreach (var id in TableData.Keys)
            {
                if (id == RowId)
                {
                    found = true;
                    continue;
                }

                if (found)
                {
                    RowId = id;
                    return TableData[id].Columns;
                }
            }

            return null;
        }

        public void Update(long rowid, Dictionary<int, BaseValue> columns)
        {
            if (TableData.TryGetValue(rowid, out JsonTableRow row))
            {
                foreach (var col in columns)
                {
                    row.Columns[col.Key] = col.Value;
                }
            }
        }
    }
}
