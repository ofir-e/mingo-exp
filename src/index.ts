import unkinkPolygon from '@turf/unkink-polygon';
import AsyncLock from 'async-lock';
import dayjs from 'dayjs';
import { deasyncObj } from 'deasync-obj';
import _ from 'lodash';
import Cache from 'memory-cache';
import { Aggregator } from 'mingo';
import { computeValue, ExpressionOperator, OperatorType, Options, PipelineOperator, useOperators } from 'mingo/core';
import { $addFields, $unset } from 'mingo/operators/pipeline';
import { RawObject } from 'mingo/types';
import hash from 'object-hash';
import { v4 as uuidv4 } from 'uuid';
import { Geometry, GeometryCollection, Point } from 'wkx';
const lock = new AsyncLock();

type ParseFunc = (...args: any[]) => any;

type FuncVal = string | number | null | undefined;

const defaultCustomFunctions = {
  date: (val: FuncVal) => dayjs(val).toISOString(),
  number: (val: FuncVal) => _.toNumber(val),
  text: (val: FuncVal) => _.toString(val).trim(),
  boolean: (val: FuncVal) => {
    if (_.isBoolean(val)) return val;
    if (!_.isString(val)) return undefined;
    if (val.toLowerCase() === 'true') return true;
    if (val.toLowerCase() === 'false') return false;
    return undefined;
  },
  point: (longitude: number, latitude: number) => new Point(longitude, latitude).toWkt(),
  wkt: (wktString: string) => {
    const geometry = Geometry.parse(wktString);
    const { type, coordinates } = geometry.toGeoJSON() as { type: any, coordinates: any[] };
    if (['Polygon', 'MultiPolygon', 'Feature', 'FeatureCollection'].includes(type)) {
      const { features } = unkinkPolygon({ type, coordinates });
      if (features.length > 1) {
        const geometries = features.map(({ geometry }) => Geometry.parseGeoJSON(geometry));
        return new GeometryCollection(geometries).toWkt();
      }
    }
    return geometry.toWkt();
  },
  geoJson: (obj: any) => Geometry.parseGeoJSON(obj).toWkt(),
  orDefault: (val: unknown, defaultVal: unknown) => val ?? defaultVal,
  concat: (separator: string, ...args: FuncVal[]) => _.compact(args).join(separator)
}

/**
 * alternative for generateCustomOperator, generate single operator that can execute custom functions by type and arguments
 * @augments additionalCustomFunctions object with parsing functions that can be accessed by their type(key in this object) and args, you can pass any function you want to use in aggregations, async functions are supported using AsyncAggregator 
 * also gives useful default parsing functions
 * @returns default type-> date: (val) => string // ISOString using dayjs
 * @returns default type-> number: (val) => number
 * @returns default type-> text: (val) => string // trimmed string
 * @returns default type-> boolean: (val: string | boolean) => boolean // support for boolean or string representations: 'TRUE' 'False' 'true' false 
 * @returns default type-> point: (longitude: number, latitude: number) => string // wkt string
 * @returns default type-> wkt: (wktString: string) => string // valid wkt string (without self intersections using '@turf/unkink-polygon')
 * @returns default type-> geoJson: (obj) => string // wkt string
 * @returns default type-> orDefault: (val, defaultVal) => unknown
 * @returns default type-> concat: (separator: string, ...args: string[]) => string

 * @example const { $customParse } = customParseExpression({
  multAsync: async (num1: number, num2: number) => {
    await delay(1000);
    return num1*num2;
  },
  lodashGet: _.get
});
 * @example useOperators(OperatorType.EXPRESSION, { $customParse }); //must do to be able to use in aggregations
 * @example {
      $project: {
        id: '$a.b',
        response: { type: 'multAsync', args: ['$a.b', 5] } // this will run $multAsync(doc.a.b, 5), arguments are passed by order in an array
      }
    },...
 *
 */
export function customParseExpression(additionalCustomFunctions: Record<string, ParseFunc> = {}) {
  const customFunctions = { ...defaultCustomFunctions, ...additionalCustomFunctions };
  const $customParse: ExpressionOperator = (obj: unknown, expr: unknown, options?: Options) => {
    const computedValue = computeValue(obj, expr, undefined, options) as { type: string, args: unknown[] };
    return (customFunctions as Record<string, ParseFunc>)[computedValue.type](...(computedValue.args));
  };
  return {
    customFunctions,
    $customParse
  }
}

/**
 * simplified operation custom generation 
 * @augments customOperators object with keys that are names for custom operators and values are any function you want to use in aggregation, async functions are supported using AsyncAggregator
 * @example const { $multAsync, $lodashGet } = generateCustomOperator({
  $multAsync: async (num1: number, num2: number) => {
    await delay(1000);
    return num1*num2;
  },
  $lodashGet: _.get
});
 * @example useOperators(OperatorType.EXPRESSION, { $multAsync, $lodashGet }); //must do to be able to use in aggregations
 * @example {
      $project: {
        id: '$a.b',
        response: { $multAsync: ['$a.b', 5] } // this will run $multAsync(doc.a.b, 5), arguments are passed by order in an array
      }
    },...
 *
 */
export function generateCustomOperator<T extends Record<string, ParseFunc>>(customOperators: T) {
  const generatedCustomOperators: Record<keyof T, (obj: unknown, args: unknown[], options?: Options) => unknown> = _.clone(customOperators);
  Object.keys(generatedCustomOperators).forEach(key => {
    generatedCustomOperators[key as keyof T] = (obj: unknown, args: unknown[], options?: Options) => {
      const computedArgs = computeValue(obj, args, undefined, options) as unknown[];
      return customOperators[key](...(computedArgs));
    }
  });
  return generatedCustomOperators as Record<string, ExpressionOperator>;
}

const MEMORY_CACHE_TIMEOUT = 600000;

/**
 * custom AsyncAggregator expression, caches request based on operation and its arguments, run it only once and returns the same response
 * no need to call useOperators(OperatorType.EXPRESSION, { $runOnce }) because AsyncAggregator calls it already
 * @example {
      $project: {
        id: '$a.b',
        response: { $runOnce: { $someAsyncCustomOperator: ['$ids'] } } // this will run someAsyncCustomOperator(ids) only once and use the response for every doc that called someAsyncCustomOperator(ids) with the same ids
      }
    },...
 *
 */
export async function $runOnce(obj: unknown, args: unknown[], options?: Options) {
  const pipelineId = computeValue({ pipelineId: '$pipelineId' }, '$pipelineId', undefined, options) as string
  const argsHash = hash(args);
  const cacheKey = `runOnce_${pipelineId}${argsHash}`;

  return lock.acquire(cacheKey, function () {
    let existingResponse = Cache.get(cacheKey);
    if (_.isNil(existingResponse)) {
      existingResponse = computeValue(obj, args, undefined, options);
      Cache.put(cacheKey, existingResponse, MEMORY_CACHE_TIMEOUT);
    }
    return existingResponse;
  })
}

/**
 * custom AsyncAggregator expression, collects values to an array
 * no need to call useOperators(OperatorType.EXPRESSION, { $collect }) because AsyncAggregator calls it already
 * @example {
      $project: {
        id: '$a.b',
        idsForBatch: { $collect: '$a.b' } // this will put in every doc an array named idsForBatch with every value of $a.b
      }
    },...
 *
 */
export async function $collect(obj: unknown, args: unknown[], options?: Options) {
  const pipelineId = computeValue({ pipelineId: '$pipelineId' }, '$pipelineId', undefined, options) as string
  const argsHash = hash(args);
  const cacheKey = `collect_${pipelineId}${argsHash}`;

  return lock.acquire(cacheKey, function () {
    let existingResponse = Cache.get(cacheKey);
    if (_.isNil(existingResponse)) {
      existingResponse = [computeValue(obj, args, undefined, options)];
      Cache.put(cacheKey, existingResponse, MEMORY_CACHE_TIMEOUT);
    } else {
      existingResponse.push(computeValue(obj, args, undefined, options));
    }
    return existingResponse;
  })
}



/**
 * Provides functionality for the mongoDB aggregation pipeline
 * similar to Aggregator but with async support (awaits all promises after each step in the aggregation)
 * uses $collect and $runOnce custom expression operators by default
 *
 * @param pipeline an Array of pipeline operators
 * @param options An optional Options to pass the aggregator
 * @constructor
 */
export class AsyncAggregator {
  constructor(private readonly pipeline: Array<RawObject>, private readonly options?: Options) {
    useOperators(OperatorType.PIPELINE, { $addFields, $unset } as Record<string,PipelineOperator>);
    useOperators(OperatorType.EXPRESSION, { $runOnce, $collect } as Record<string,ExpressionOperator>);
  }

  private addPipelineIdPipe() {
    const pipelineId = uuidv4();
    return { $addFields: { pipelineId } };
  }

  private unsetPipelineIdPipe() {
    return { $unset: 'pipelineId' };
  }

  async run(collection: Array<RawObject>): Promise<Array<RawObject>> {

    const aggregators = this.pipeline.map(pipe => new Aggregator([this.addPipelineIdPipe(), pipe, this.unsetPipelineIdPipe()], this.options));
    for (const agg of aggregators) {
      collection = agg.run(collection);
      await deasyncObj(collection);
    }
    return collection;
  }
}