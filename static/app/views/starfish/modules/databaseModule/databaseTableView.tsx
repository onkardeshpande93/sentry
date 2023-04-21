import {useQuery} from '@tanstack/react-query';
import {Location} from 'history';

import GridEditable, {GridColumnHeader} from 'sentry/components/gridEditable';
import Link from 'sentry/components/links/link';

const HOST = 'http://localhost:8080';

type Props = {
  location: Location;
  onSelect: (row: DataRow) => void;
  transaction: string;
  action?: string;
  table?: string;
};

export type DataRow = {
  desc: string;
  epm: number;
  p75: number;
  total_time: number;
  transactions: number;
};

const COLUMN_ORDER = [
  {
    key: 'desc',
    name: 'Query',
    width: 600,
  },
  {
    key: 'epm',
    name: 'tpm',
  },
  {
    key: 'p75',
    name: 'p75',
  },
  {
    key: 'transactions',
    name: 'transactions',
  },
  {
    key: 'total_time',
    name: 'Total Time',
  },
];

export default function APIModuleView({
  location,
  action,
  transaction,
  onSelect,
  table,
}: Props) {
  const transactionFilter =
    transaction.length > 0 ? `transaction='${transaction}'` : null;
  const tableFilter = table ? `domain = '${table}'` : null;
  const actionFilter = action ? `action = '${action}'` : null;

  const filters = [
    `startsWith(span_operation, 'db')`,
    `span_operation != 'db.redis'`,
    transactionFilter,
    tableFilter,
    actionFilter,
  ].filter(fil => !!fil);
  const TABLE_LIST_QUERY = `select description as desc, (divide(count(), divide(1209600.0, 60)) AS epm), quantile(0.75)(exclusive_time) as p75,
    uniq(transaction) as transactions,
    sum(exclusive_time) as total_time
    from default.spans_experimental_starfish
    where
    ${filters.join(' and ')}
    group by description
    order by -pow(10, floor(log10(count()))), -quantile(0.5)(exclusive_time)
    limit 100
  `;

  console;

  const {isLoading: areEndpointsLoading, data: endpointsData} = useQuery({
    queryKey: ['endpoints', action, transaction, table],
    queryFn: () => fetch(`${HOST}/?query=${TABLE_LIST_QUERY}`).then(res => res.json()),
    retry: false,
    initialData: [],
  });

  function renderHeadCell(column: GridColumnHeader): React.ReactNode {
    return <span>{column.name}</span>;
  }

  function renderBodyCell(column: GridColumnHeader, row: DataRow): React.ReactNode {
    if (column.key === 'desc') {
      return (
        <Link onClick={() => onSelect(row)} to="">
          {row[column.key]}
        </Link>
      );
    }
    return <span>{row[column.key]}</span>;
  }

  return (
    <GridEditable
      isLoading={areEndpointsLoading}
      data={endpointsData}
      columnOrder={COLUMN_ORDER}
      columnSortBy={[]}
      grid={{
        renderHeadCell,
        renderBodyCell,
      }}
      location={location}
    />
  );
}
