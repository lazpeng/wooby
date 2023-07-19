using System;
using System.Collections.Generic;
using System.Linq;
using wooby.Database.Persistence;
using wooby.Error;

namespace wooby.Database.Defaults;

public class DualDataProvider : ITableDataProvider
{
    public long Delete(long rowId)
    {
        throw new Exception("Not supported");
    }

    public long Insert(Dictionary<int, BaseValue> values)
    {
        throw new Exception("Not supported");
    }

    public void Update(long rowId, Dictionary<int, BaseValue> columns)
    {
        throw new Exception("Not supported");
    }

    public IEnumerable<BaseValue>? Seek(long rowId)
    {
        return rowId == 0 ? new List<BaseValue>() : null;
    }

    public IEnumerable<BaseValue>? SeekNext(ref long rowId)
    {
        if (rowId < 0)
        {
            rowId = 0;
            return Seek(0);
        }
        return null;
    }

    public void Initialize(Context context, TableMeta meta)
    {
    }
}

public class InMemoryDataProvider : ITableDataProvider
{
    private long _lastRowId = -1;
    private TableMeta? _meta;

    private struct Row
    {
        public long RowId;
        public List<BaseValue> Columns;
    }

    private List<Row> _rows = new();

    public void SetupRows(IEnumerable<List<BaseValue>> values)
    {
        foreach (var row in values)
        {
            _rows.Add(new Row {RowId = ++_lastRowId, Columns = row});
        }
    }

    private Row? Find(long rowId, out int index)
    {
        for (var i = 0; i < _rows.Count; ++i)
        {
            var row = _rows[i];
            if (row.RowId != rowId && rowId != long.MinValue) continue;
            
            index = i;
            return row;
        }

        index = -1;
        return null;
    }

    IEnumerable<BaseValue>? ITableDataProvider.Seek(long rowId)
    {
        return Find(rowId, out _)?.Columns;
    }

    public IEnumerable<BaseValue>? SeekNext(ref long rowId)
    {
        Find(rowId, out var index);

        if (rowId != long.MinValue)
        {
            index += 1;
        }

        if (_rows.Count > index)
        {
            var row = _rows[index];
            rowId = row.RowId;
            return row.Columns;
        }

        rowId = long.MinValue;
        return null;
    }

    public long Delete(long rowId)
    {
        Find(rowId, out var index);
        _rows.RemoveAt(index);
        if (index == 0 || index >= _rows.Count)
        {
            return long.MinValue;
        }
        return _rows[index - 1].RowId;
    }

    public long Insert(Dictionary<int, BaseValue> values)
    {
        if (_meta == null)
        {
            throw new WoobyDatabaseException("Data provider is not initialized");
        }
        
        var row = new Row {RowId = ++_lastRowId, Columns = new List<BaseValue>(_meta.Columns.Count)};

        for (var idx = 0; idx < _meta.Columns.Count; ++idx)
        {
            row.Columns.Add(values.TryGetValue(idx, out var v) ? v : new NullValue());
        }

        _rows.Add(row);

        return row.RowId;
    }

    public void Update(long rowId, Dictionary<int, BaseValue> columns)
    {
        var row = Find(rowId, out var _);

        if (row == null) return;
        foreach (var col in columns)
        {
            row.Value.Columns[col.Key] = col.Value;
        }
    }

    public virtual void Initialize(Context context, TableMeta meta)
    {
        _rows = new List<Row>();
        _meta = meta;
    }
}

public class LoveLiveDataProvider : InMemoryDataProvider
{
    private class Group
    {
        public long Id;
        public long? ParentId;
        public string Nome = string.Empty;
        public int Ano;
        public int NumIntegrantes;
    }

    private static readonly List<Group> Groups = new()
    {
        new Group {Id = 0, Nome = "μ's", Ano = 2010, NumIntegrantes = 9},
        new Group {Id = 1, Nome = "Aqours", Ano = 2016, NumIntegrantes = 9},
        new Group {Id = 2, Nome = "Nijigasaki School Idol Club", Ano = 2017, NumIntegrantes = 10},
        new Group {Id = 3, Nome = "Liella", Ano = 2020, NumIntegrantes = 5},
        new Group {Id = 4, Nome = "BiBi", Ano = 2011, NumIntegrantes = 3, ParentId = 0},
        new Group {Id = 5, Nome = "Lily white", Ano = 2011, NumIntegrantes = 3, ParentId = 0},
        new Group {Id = 6, Nome = "Printemps", Ano = 2011, NumIntegrantes = 3, ParentId = 0},
        new Group {Id = 7, Nome = "Guilty kiss", Ano = 2016, NumIntegrantes = 3, ParentId = 1},
        new Group {Id = 8, Nome = "CYaRon", Ano = 2016, NumIntegrantes = 3, ParentId = 1},
        new Group {Id = 9, Nome = "AZALEA", Ano = 2016, NumIntegrantes = 3, ParentId = 1},
    };

    public override void Initialize(Context context, TableMeta meta)
    {
        base.Initialize(context, meta);
        SetupRows(Groups.Select(g =>
            new List<BaseValue>
            {
                new NumberValue(g.Id),
                g.ParentId != null ? new NumberValue(g.ParentId.Value) : new NullValue(),
                new TextValue(g.Nome),
                new NumberValue(g.Ano),
                new NumberValue(g.NumIntegrantes)
            }).ToList());
    }
}