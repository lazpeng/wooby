﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby
{
    public enum ColumnType
    {
        String,
        Number,
        Boolean
    }

    public class ColumnMeta
    {
        public string Name { get; set; }
        public int Id { get; set; }
        public bool Unique { get; set; }
        public ColumnType Type { get; set; }
        public TableMeta Parent { get; set; }
    }

    public class TableMeta
    {
        public string Name { get; set; }
        public long Id { get; set; }
        public List<ColumnMeta> Columns { get; set; } = new List<ColumnMeta>();
        public bool IsReal { get; set; }
        public bool IsTemporary { get; set; }
    }

    public class GlobalVariable
    {
        public string Name { get; set; }
        public long Id { get; set; }
        public ColumnType Type { get; set; }
    }

    public class Context
    {
        public List<TableMeta> Tables { get; set; } = new List<TableMeta>();
        public List<GlobalVariable> Variables { get; private set; } = new List<GlobalVariable>();

        public void AddColumn(ColumnMeta column, TableMeta target)
        {
            column.Parent = target;

            int id = 0;
            if (target.Columns.Count > 0)
            {
                id = target.Columns.Max(c => c.Id) + 1;
            }
            column.Id = id;
            target.Columns.Add(column);
        }

        public void AddTable(TableMeta table)
        {
            long id = 0;
            if (Tables.Count > 0)
            {
                id = Tables.Max(t => t.Id) + 1;
            }
            table.Id = id;
            Tables.Add(table);
        }

        public TableMeta FindTable(ColumnReference reference)
        {
            return Tables.Find(t => t.Name.ToUpper() == reference.Table.ToUpper());
        }

        public ColumnMeta FindColumn(ColumnReference reference)
        {
            var tableMeta = FindTable(reference);

            if (tableMeta != null)
            {
                return tableMeta.Columns.Find(c => c.Name.ToUpper() == reference.Column.ToUpper());
            }

            return null;
        }

        public GlobalVariable FindVariable(string Name)
        {
            return Variables.Find(v => v.Name.ToUpper() == Name.ToUpper());
        }

        public bool IsReferenceValid(ColumnReference reference)
        {
            return FindColumn(reference) != null ||
                (string.IsNullOrEmpty(reference.Table) && FindVariable(reference.Column) != null) ||
                (reference.Column == "*" && FindTable(reference) != null);
        }
    }
}
