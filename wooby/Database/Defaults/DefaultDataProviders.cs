using System;
using System.Collections.Generic;
using System.Linq;
using wooby.Database.Persistence;

namespace wooby.Database.Defaults;

public class Dual_DataProvider : ITableDataProvider
{
    public long Delete(long rowid)
    {
        throw new Exception("Not supported");
    }

    public long Insert(Dictionary<int, BaseValue> values)
    {
        throw new Exception("Not supported");
    }

    public void Update(long rowid, Dictionary<int, BaseValue> columns)
    {
        throw new Exception("Not supported");
    }

    public IEnumerable<BaseValue> Seek(long RowId)
    {
        if (RowId == 0)
        {
            return new List<BaseValue>();
        }
        else return null;
    }

    public IEnumerable<BaseValue> SeekNext(ref long RowId)
    {
        if (RowId < 0)
        {
            RowId = 0;
            return Seek(0);
        }
        else return null;
    }

    public void Initialize(Context context, TableMeta meta)
    {
    }
}

public class InMemory_DataProvider : ITableDataProvider
{
    private long LastRowId = -1;
    private TableMeta Meta;

    struct Row
    {
        public long RowId;
        public List<BaseValue> Columns;
    }

    private List<Row> Rows;

    protected void SetupRows(List<List<BaseValue>> Values)
    {
        foreach (var row in Values)
        {
            Rows.Add(new Row {RowId = ++LastRowId, Columns = row});
        }
    }

    private Row? Find(long RowId, out int index)
    {
        for (int i = 0; i < Rows.Count; ++i)
        {
            var row = Rows[i];
            if (row.RowId == RowId || RowId == long.MinValue)
            {
                index = i;
                return row;
            }
        }

        index = -1;

        return null;
    }

    IEnumerable<BaseValue> ITableDataProvider.Seek(long RowId)
    {
        return Find(RowId, out int _)?.Columns;
    }

    public IEnumerable<BaseValue> SeekNext(ref long RowId)
    {
        Find(RowId, out int index);

        if (RowId != long.MinValue)
        {
            index += 1;
        }

        if (Rows.Count > index)
        {
            var row = Rows[index];
            RowId = row.RowId;
            return row.Columns;
        }

        RowId = long.MinValue;
        return null;
    }

    public long Delete(long rowid)
    {
        Find(rowid, out int index);
        Rows.RemoveAt(index);
        if (index == 0 || index >= Rows.Count)
        {
            return long.MinValue;
        }
        else
        {
            return Rows[index - 1].RowId;
        }
    }

    public long Insert(Dictionary<int, BaseValue> values)
    {
        var row = new Row {RowId = ++LastRowId, Columns = new List<BaseValue>(Meta.Columns.Count)};

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

        Rows.Add(row);

        return row.RowId;
    }

    public void Update(long rowid, Dictionary<int, BaseValue> columns)
    {
        var row = Find(rowid, out int _);

        if (row != null)
        {
            foreach (var col in columns)
            {
                row.Value.Columns[col.Key] = col.Value;
            }
        }
    }

    public virtual void Initialize(Context context, TableMeta meta)
    {
        Rows = new List<Row>();
        Meta = meta;
    }
}

public class LoveLive_DataProvider : InMemory_DataProvider
{
    private class Group
    {
        public long Id;
        public long? ParentId;
        public string Nome;
        public int Ano;
        public int NumIntegrantes;
    }

    private static readonly List<Group> Groups = new()
    {
        new Group() {Id = 0, Nome = "μ's", Ano = 2010, NumIntegrantes = 9},
        new Group() {Id = 1, Nome = "Aqours", Ano = 2016, NumIntegrantes = 9},
        new Group() {Id = 2, Nome = "Nijigasaki School Idol Club", Ano = 2017, NumIntegrantes = 10},
        new Group() {Id = 3, Nome = "Liella", Ano = 2020, NumIntegrantes = 5},
        new Group() {Id = 4, Nome = "BiBi", Ano = 2011, NumIntegrantes = 3, ParentId = 0},
        new Group() {Id = 5, Nome = "Lily white", Ano = 2011, NumIntegrantes = 3, ParentId = 0},
        new Group() {Id = 6, Nome = "Printemps", Ano = 2011, NumIntegrantes = 3, ParentId = 0},
        new Group() {Id = 7, Nome = "Guilty kiss", Ano = 2016, NumIntegrantes = 3, ParentId = 1},
        new Group() {Id = 8, Nome = "CYaRon", Ano = 2016, NumIntegrantes = 3, ParentId = 1},
        new Group() {Id = 9, Nome = "AZALEA", Ano = 2016, NumIntegrantes = 3, ParentId = 1},
    };

    public override void Initialize(Context context, TableMeta meta)
    {
        base.Initialize(context, meta);
        SetupRows(Groups.Select(g =>
            new List<BaseValue>()
            {
                new NumberValue(g.Id),
                g.ParentId != null ? new NumberValue(g.ParentId.Value) : new NullValue(),
                new TextValue(g.Nome),
                new NumberValue(g.Ano),
                new NumberValue(g.NumIntegrantes)
            }).ToList());
    }
}