import {
  Fragment,
  ReactElement,
  useEffect,
  useLayoutEffect,
  useMemo,
  useState,
} from 'react';
import styled from '@emotion/styled';
import * as Sentry from '@sentry/react';
import {mat3, vec2} from 'gl-matrix';

import {Button} from 'sentry/components/button';
import {FlamegraphZoomView} from 'sentry/components/profiling/flamegraph/flamegraphZoomView';
import {Flex} from 'sentry/components/profiling/flex';
import SwitchButton from 'sentry/components/switchButton';
import {t} from 'sentry/locale';
import {space} from 'sentry/styles/space';
import {defined} from 'sentry/utils';
import {
  CanvasPoolManager,
  useCanvasScheduler,
} from 'sentry/utils/profiling/canvasScheduler';
import {CanvasView} from 'sentry/utils/profiling/canvasView';
import {Flamegraph as FlamegraphModel} from 'sentry/utils/profiling/flamegraph';
import {useFlamegraphPreferences} from 'sentry/utils/profiling/flamegraph/hooks/useFlamegraphPreferences';
import {useFlamegraphProfiles} from 'sentry/utils/profiling/flamegraph/hooks/useFlamegraphProfiles';
import {useDispatchFlamegraphState} from 'sentry/utils/profiling/flamegraph/hooks/useFlamegraphState';
import {
  useFlamegraphTheme,
  useMutateFlamegraphTheme,
} from 'sentry/utils/profiling/flamegraph/useFlamegraphTheme';
import {FlamegraphCanvas} from 'sentry/utils/profiling/flamegraphCanvas';
import {FlamegraphFrame} from 'sentry/utils/profiling/flamegraphFrame';
import {
  computeConfigViewWithStrategy,
  useResizeCanvasObserver,
} from 'sentry/utils/profiling/gl/utils';
import {FlamegraphRendererWebGL} from 'sentry/utils/profiling/renderers/flamegraphRendererWebGL';
import {Rect} from 'sentry/utils/profiling/speedscope';
import {useDevicePixelRatio} from 'sentry/utils/useDevicePixelRatio';
import {useProfileGroup} from 'sentry/views/profiling/profileGroupProvider';

const LOADING_OR_FALLBACK_FLAMEGRAPH = FlamegraphModel.Empty();

interface AggregateFlamegraphProps {
  hideSystemFrames: boolean;
  setHideSystemFrames: (hideSystemFrames: boolean) => void;
  hideToolbar?: boolean;
}

export function AggregateFlamegraph(props: AggregateFlamegraphProps): ReactElement {
  const devicePixelRatio = useDevicePixelRatio();
  const dispatch = useDispatchFlamegraphState();

  const profileGroup = useProfileGroup();

  const flamegraphTheme = useFlamegraphTheme();
  const setFlamegraphThemeMutation = useMutateFlamegraphTheme();
  const profiles = useFlamegraphProfiles();
  const {colorCoding, sorting, view} = useFlamegraphPreferences();
  const {threadId} = profiles;

  const [flamegraphCanvasRef, setFlamegraphCanvasRef] =
    useState<HTMLCanvasElement | null>(null);
  const [flamegraphOverlayCanvasRef, setFlamegraphOverlayCanvasRef] =
    useState<HTMLCanvasElement | null>(null);

  const canvasPoolManager = useMemo(() => new CanvasPoolManager(), []);
  const scheduler = useCanvasScheduler(canvasPoolManager);

  const profile = useMemo(() => {
    return profileGroup.profiles.find(p => p.threadId === threadId);
  }, [profileGroup, threadId]);

  const flamegraph = useMemo(() => {
    if (typeof threadId !== 'number') {
      return LOADING_OR_FALLBACK_FLAMEGRAPH;
    }

    // This could happen if threadId was initialized from query string, but for some
    // reason the profile was removed from the list of profiles.
    if (!profile) {
      return LOADING_OR_FALLBACK_FLAMEGRAPH;
    }

    const transaction = Sentry.startTransaction({
      op: 'import',
      name: 'flamegraph.constructor',
    });

    transaction.setTag('sorting', sorting.split(' ').join('_'));
    transaction.setTag('view', view.split(' ').join('_'));

    const newFlamegraph = new FlamegraphModel(profile, threadId, {
      inverted: view === 'bottom up',
      sort: sorting,
      configSpace: undefined,
    });

    transaction.finish();

    return newFlamegraph;
  }, [profile, sorting, threadId, view]);

  const flamegraphCanvas = useMemo(() => {
    if (!flamegraphCanvasRef) {
      return null;
    }
    const yOrigin = flamegraphTheme.SIZES.TIMELINE_HEIGHT * devicePixelRatio;
    return new FlamegraphCanvas(flamegraphCanvasRef, vec2.fromValues(0, yOrigin));
  }, [devicePixelRatio, flamegraphCanvasRef, flamegraphTheme]);

  const flamegraphView = useMemo<CanvasView<FlamegraphModel> | null>(
    () => {
      if (!flamegraphCanvas) {
        return null;
      }

      const newView = new CanvasView({
        canvas: flamegraphCanvas,
        model: flamegraph,
        options: {
          inverted: flamegraph.inverted,
          minWidth: flamegraph.profile.minFrameDuration,
          barHeight: flamegraphTheme.SIZES.BAR_HEIGHT,
          depthOffset: flamegraphTheme.SIZES.FLAMEGRAPH_DEPTH_OFFSET,
          configSpaceTransform: undefined,
        },
      });

      // Set to 3/4 of the view up, magic number... Would be best to comput some weighted visual score
      // based on the number of frames and the depth of the frames, but lets see if we can make it work
      // with this for now
      newView.setConfigView(newView.configView.withY(newView.configView.height * 0.75));
      return newView;
    },

    // We skip position.view dependency because it will go into an infinite loop
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [flamegraph, flamegraphCanvas, flamegraphTheme]
  );

  useEffect(() => {
    const canvasHeight = flamegraphCanvas?.logicalSpace.height;
    if (!canvasHeight) {
      return;
    }

    setFlamegraphThemeMutation(theme => {
      const flamegraphFitTo = canvasHeight / flamegraph.depth;
      const minReadableRatio = 0.8; // this is quite small
      const fitToRatio = flamegraphFitTo / theme.SIZES.BAR_HEIGHT;
      const barHeightRatio = Math.min(Math.max(minReadableRatio, fitToRatio), 1.2);

      // reduce the offset to leave just enough space for the toolbar
      theme.SIZES.FLAMEGRAPH_DEPTH_OFFSET = 2.5;
      theme.SIZES.BAR_HEIGHT = theme.SIZES.BAR_HEIGHT * barHeightRatio;
      theme.SIZES.BAR_FONT_SIZE = theme.SIZES.BAR_FONT_SIZE * barHeightRatio;
      return theme;
    });

    // We skip `flamegraphCanvas` as it causes an infinite loop
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [flamegraph, setFlamegraphThemeMutation, flamegraphCanvas?.logicalSpace.height]);

  // Uses a useLayoutEffect to ensure that these top level/global listeners are added before
  // any of the children components effects actually run. This way we do not lose events
  // when we register/unregister these top level listeners.
  useLayoutEffect(() => {
    if (!flamegraphCanvas || !flamegraphView) {
      return undefined;
    }

    // This code below manages the synchronization of the config views between spans and flamegraph
    // We do so by listening to the config view change event and then updating the other views accordingly which
    // allows us to keep the X axis in sync between the two views but keep the Y axis independent
    const onConfigViewChange = (rect: Rect, sourceConfigViewChange: CanvasView<any>) => {
      if (sourceConfigViewChange === flamegraphView) {
        flamegraphView.setConfigView(rect.withHeight(flamegraphView.configView.height));
      }

      canvasPoolManager.draw();
    };

    const onTransformConfigView = (
      mat: mat3,
      sourceTransformConfigView: CanvasView<any>
    ) => {
      if (sourceTransformConfigView === flamegraphView) {
        flamegraphView.transformConfigView(mat);
      }

      canvasPoolManager.draw();
    };

    const onResetZoom = () => {
      flamegraphView.resetConfigView(flamegraphCanvas);

      canvasPoolManager.draw();
    };

    const onZoomIntoFrame = (frame: FlamegraphFrame, strategy: 'min' | 'exact') => {
      const newConfigView = computeConfigViewWithStrategy(
        strategy,
        flamegraphView.configView,
        new Rect(frame.start, frame.depth, frame.end - frame.start, 1)
      ).transformRect(flamegraphView.configSpaceTransform);

      flamegraphView.setConfigView(newConfigView);

      canvasPoolManager.draw();
    };

    scheduler.on('set config view', onConfigViewChange);
    scheduler.on('transform config view', onTransformConfigView);
    scheduler.on('reset zoom', onResetZoom);
    scheduler.on('zoom at frame', onZoomIntoFrame);

    return () => {
      scheduler.off('set config view', onConfigViewChange);
      scheduler.off('transform config view', onTransformConfigView);
      scheduler.off('reset zoom', onResetZoom);
      scheduler.off('zoom at frame', onZoomIntoFrame);
    };
  }, [canvasPoolManager, flamegraphCanvas, flamegraphView, scheduler]);

  const flamegraphCanvases = useMemo(() => {
    return [flamegraphCanvasRef, flamegraphOverlayCanvasRef];
  }, [flamegraphCanvasRef, flamegraphOverlayCanvasRef]);

  const flamegraphCanvasBounds = useResizeCanvasObserver(
    flamegraphCanvases,
    canvasPoolManager,
    flamegraphCanvas,
    flamegraphView
  );

  const flamegraphRenderer = useMemo(() => {
    if (!flamegraphCanvasRef) {
      return null;
    }

    return new FlamegraphRendererWebGL(flamegraphCanvasRef, flamegraph, flamegraphTheme, {
      colorCoding,
      draw_border: true,
    });
  }, [colorCoding, flamegraph, flamegraphCanvasRef, flamegraphTheme]);

  useEffect(() => {
    if (defined(profiles.threadId)) {
      return;
    }
    const threadID =
      typeof profileGroup.activeProfileIndex === 'number'
        ? profileGroup.profiles[profileGroup.activeProfileIndex]?.threadId
        : null;
    // fall back case, when we finally load the active profile index from the profile,
    // make sure we update the thread id so that it is show first
    if (defined(threadID)) {
      dispatch({
        type: 'set thread id',
        payload: threadID,
      });
    }
  }, [profileGroup, profiles.threadId, dispatch]);

  return (
    <Fragment>
      <FlamegraphZoomView
        canvasBounds={flamegraphCanvasBounds}
        canvasPoolManager={canvasPoolManager}
        flamegraph={flamegraph}
        flamegraphRenderer={flamegraphRenderer}
        flamegraphCanvas={flamegraphCanvas}
        flamegraphCanvasRef={flamegraphCanvasRef}
        flamegraphOverlayCanvasRef={flamegraphOverlayCanvasRef}
        flamegraphView={flamegraphView}
        setFlamegraphCanvasRef={setFlamegraphCanvasRef}
        setFlamegraphOverlayCanvasRef={setFlamegraphOverlayCanvasRef}
        disablePanX
        disableZoom
        disableGrid
        disableCallOrderSort
      />
      {props.hideToolbar ? null : (
        <AggregateFlamegraphToolbar>
          <Flex justify="space-between" align="center">
            <Button size="xs" onClick={() => scheduler.dispatch('reset zoom')}>
              {t('Reset Zoom')}
            </Button>
            <Flex align="center" gap={space(1)}>
              <span>{t('Hide System Frames')}</span>
              <SwitchButton
                toggle={() => props.setHideSystemFrames(!props.hideSystemFrames)}
                isActive={props.hideSystemFrames}
              />
            </Flex>
          </Flex>
        </AggregateFlamegraphToolbar>
      )}
    </Fragment>
  );
}

const AggregateFlamegraphToolbar = styled('div')`
  position: absolute;
  left: 0;
  top: 0;
  padding: ${space(1)};
  padding-left: ${space(1)};
  background-color: rgba(255, 255, 255, 0.6);
  width: 100%;
`;
