using System;
using System.Collections.Generic;
using System.Linq;
using wooby.Error;

namespace wooby.Database.Persistence.Json;

public class JsonTableDataProvider : ITableDataProvider
{
    private JsonCustomData? _privateData;
    private Dictionary<long, JsonTableRow>? _tableData;
    private TableMeta? _meta;

    public long Delete(long rowId)
    {
        if (_tableData == null)
        {
            throw new WoobyException("Provider is not initialized");
        }
        
        var index = _tableData.Keys.ToList().IndexOf(rowId);
        _tableData.Remove(rowId);

        if (_tableData.Count == 0 || index >= _tableData.Count)
        {
            return long.MinValue;
        }
        return _tableData.Keys.ToList()[index];
    }

    public void Initialize(Context context, TableMeta meta)
    {
        if (context.CustomSourceData is JsonCustomData json)
        {
            _privateData = json;
            _meta = meta;
            if (_privateData.Data.TryGetValue(meta.Id, out var rows))
            {
                _tableData = rows;
            } else
            {
                _tableData = new Dictionary<long, JsonTableRow>();
                _privateData.Data.Add(meta.Id, _tableData);
            }
        } else
        {
            throw new Exception("Failure to initialize JsonTableDataProvider: Custom data is not available");
        }
    }

    public long Insert(Dictionary<int, BaseValue> values)
    {
        if (_tableData == null || _privateData == null || _meta == null)
        {
            throw new WoobyException("Provider is not initialized");
        }

        var row = new JsonTableRow { RowId = _privateData.NextRowId++, Columns = new List<BaseValue>(_meta.Columns.Count) };

        for (var idx = 0; idx < _meta.Columns.Count; ++idx)
        {
            row.Columns.Add(values.TryGetValue(idx, out var v) ? v : new NullValue());
        }

        _tableData.Add(row.RowId, row);

        return row.RowId;
    }

    public IEnumerable<BaseValue>? Seek(long rowId)
    {
        if (_tableData == null)
        {
            throw new WoobyException("Provider is not initialized");
        }

        return _tableData.TryGetValue(rowId, out var row) ? row.Columns : null;
    }

    public IEnumerable<BaseValue>? SeekNext(ref long rowId)
    {
        if (_tableData == null)
        {
            throw new WoobyException("Provider is not initialized");
        }

        var found = rowId == long.MinValue;

        foreach (var id in _tableData.Keys)
        {
            if (id == rowId)
            {
                found = true;
                continue;
            }

            if (!found) continue;
            rowId = id;
            return _tableData[id].Columns;
        }

        return null;
    }

    public void Update(long rowId, Dictionary<int, BaseValue> columns)
    {
        if (_tableData == null)
        {
            throw new WoobyException("Provider is not initialized");
        }

        if (!_tableData.TryGetValue(rowId, out var row)) return;
        foreach (var col in columns)
        {
            row.Columns[col.Key] = col.Value;
        }
    }
}