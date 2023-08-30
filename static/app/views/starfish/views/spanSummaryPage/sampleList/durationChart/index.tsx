import {useTheme} from '@emotion/react';

import {t} from 'sentry/locale';
import {EChartClickHandler, EChartHighlightHandler, Series} from 'sentry/types/echarts';
import {usePageError} from 'sentry/utils/performance/contexts/pageError';
import {AVG_COLOR} from 'sentry/views/starfish/colours';
import Chart from 'sentry/views/starfish/components/chart';
import {isNearAverage} from 'sentry/views/starfish/components/samplesTable/common';
import {useSpanMetrics} from 'sentry/views/starfish/queries/useSpanMetrics';
import {useSpanMetricsSeries} from 'sentry/views/starfish/queries/useSpanMetricsSeries';
import {SpanSample, useSpanSamples} from 'sentry/views/starfish/queries/useSpanSamples';
import {SpanMetricsField} from 'sentry/views/starfish/types';
import {DataTitles} from 'sentry/views/starfish/views/spans/types';
import {
  crossIconPath,
  downwardPlayIconPath,
  upwardPlayIconPath,
} from 'sentry/views/starfish/views/spanSummaryPage/sampleList/durationChart/symbol';

const {SPAN_SELF_TIME, SPAN_OP} = SpanMetricsField;

type Props = {
  groupId: string;
  transactionMethod: string;
  transactionName: string;
  highlightedSpanId?: string;
  onClickSample?: (sample: SpanSample) => void;
  onMouseLeaveSample?: () => void;
  onMouseOverSample?: (sample: SpanSample) => void;
  spanDescription?: string;
};

function DurationChart({
  groupId,
  transactionName,
  onClickSample,
  onMouseLeaveSample,
  onMouseOverSample,
  highlightedSpanId,
  transactionMethod,
}: Props) {
  const theme = useTheme();
  const {setPageError} = usePageError();

  const getSampleSymbol = (
    duration: number,
    compareToDuration: number
  ): {color: string; symbol: string} => {
    if (isNearAverage(duration, compareToDuration)) {
      return {
        symbol: crossIconPath,
        color: theme.gray500,
      };
    }

    return duration > compareToDuration
      ? {
          symbol: upwardPlayIconPath,
          color: theme.red300,
        }
      : {
          symbol: downwardPlayIconPath,
          color: theme.green300,
        };
  };

  const {
    isLoading,
    data: spanMetricsSeriesData,
    error: spanMetricsSeriesError,
  } = useSpanMetricsSeries(
    groupId,
    {transactionName, 'transaction.method': transactionMethod},
    [`avg(${SPAN_SELF_TIME})`],
    'api.starfish.sidebar-span-metrics-chart'
  );

  const {data: spanMetrics, error: spanMetricsError} = useSpanMetrics(
    groupId,
    {transactionName, 'transaction.method': transactionMethod},
    [`avg(${SPAN_SELF_TIME})`, SPAN_OP],
    'api.starfish.span-summary-panel-samples-table-avg'
  );

  const avg = spanMetrics?.[`avg(${SPAN_SELF_TIME})`] || 0;

  const {
    data: spans,
    isLoading: areSpanSamplesLoading,
    isRefetching: areSpanSamplesRefetching,
  } = useSpanSamples({
    groupId,
    transactionName,
    transactionMethod,
  });

  const baselineAvgSeries: Series = {
    seriesName: 'Average',
    data: [],
    markLine: {
      data: [{valueDim: 'x', yAxis: avg}],
      symbol: ['none', 'none'],
      lineStyle: {
        color: theme.gray400,
      },
      emphasis: {disabled: true},
      label: {
        fontSize: 11,
        position: 'insideEndBottom',
        formatter: () => 'Average',
      },
    },
  };

  const sampledSpanDataSeries: Series[] = spans.map(
    ({
      timestamp,
      [SPAN_SELF_TIME]: duration,
      'transaction.id': transaction_id,
      span_id,
    }) => {
      const {symbol, color} = getSampleSymbol(duration, avg);
      return {
        data: [
          {
            name: timestamp,
            value: duration,
          },
        ],
        symbol,
        color,
        symbolSize: span_id === highlightedSpanId ? 19 : 14,
        seriesName: transaction_id.substring(0, 8),
      };
    }
  );

  const getSample = (timestamp: string, duration: number) => {
    return spans.find(s => s.timestamp === timestamp && s[SPAN_SELF_TIME] === duration);
  };

  const handleChartClick: EChartClickHandler = e => {
    const isSpanSample = e?.componentSubType === 'scatter';
    if (isSpanSample && onClickSample) {
      const [timestamp, duration] = e.value as [string, number];
      const sample = getSample(timestamp, duration);
      if (sample) {
        onClickSample(sample);
      }
    }
  };

  const handleChartHighlight: EChartHighlightHandler = e => {
    const {seriesIndex} = e.batch[0];
    const isSpanSample = seriesIndex > 1;
    if (isSpanSample && onMouseOverSample) {
      const spanSampleData = sampledSpanDataSeries?.[seriesIndex - 2]?.data[0];
      const {name: timestamp, value: duration} = spanSampleData;
      const sample = getSample(timestamp as string, duration);
      if (sample) {
        onMouseOverSample(sample);
      }
    }
    if (!isSpanSample && onMouseLeaveSample) {
      onMouseLeaveSample();
    }
  };

  const handleMouseLeave = () => {
    if (onMouseLeaveSample) {
      onMouseLeaveSample();
    }
  };

  if (spanMetricsSeriesError || spanMetricsError) {
    setPageError(t('An error has occured while loading chart data'));
  }

  return (
    <div onMouseLeave={handleMouseLeave}>
      <h5>{DataTitles.avg}</h5>
      <Chart
        height={140}
        onClick={handleChartClick}
        onHighlight={handleChartHighlight}
        aggregateOutputFormat="duration"
        data={[spanMetricsSeriesData?.[`avg(${SPAN_SELF_TIME})`], baselineAvgSeries]}
        loading={isLoading}
        scatterPlot={
          areSpanSamplesLoading || areSpanSamplesRefetching
            ? undefined
            : sampledSpanDataSeries
        }
        utc={false}
        chartColors={[AVG_COLOR, 'black']}
        isLineChart
        definedAxisTicks={4}
      />
    </div>
  );
}

export default DurationChart;
