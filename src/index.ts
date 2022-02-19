import unkinkPolygon from '@turf/unkink-polygon';
import AsyncLock from 'async-lock';
import dayjs from 'dayjs';
import { deasyncObj } from 'deasync-obj';
import _ from 'lodash';
import Cache from 'memory-cache';
import { Aggregator } from 'mingo';
import { computeValue, OperatorType, Options, useOperators } from 'mingo/core';
import { $addFields, $unset } from 'mingo/operators/pipeline';
import { RawObject } from 'mingo/types';
import hash from 'object-hash';
import { v4 as uuidv4 } from 'uuid';
import { Geometry, GeometryCollection, Point } from 'wkx';
const lock = new AsyncLock();

type ParseFunc = (...args: any[]) => any;

const defaultCustomFunctions = {
  date: (val: any) => dayjs(val).toISOString(),
  number: (val: any) => _.toNumber(val),
  text: (val: any) => _.toString(val).trim(),
  boolean: (val: string | boolean) => {
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
  orDefault: (val: any, defaultVal: any) => val ?? defaultVal,
  concat: (separator: string, ...args: string[]) => _.compact(args).join(separator)
}

/**
 * alternative for generateCustomOperator, generate single operator that can execute custom functions by type and arguments
 * @augments additionalCustomFunctions object with parsing functions that can be accessed by their type(key in this object) and args, you can pass any function you want to use in aggregations, async functions are supported using AsyncAggregator 
 * also gives useful default parsing functions
 * @returns default type-> date: (val: any) => string // ISOString using dayjs
 * @returns default type-> number: (val: any) => number
 * @returns default type-> text: (val: any) => string // trimmed string
 * @returns default type-> boolean: (val: string | boolean) => boolean // support for boolean or string representations: 'TRUE' 'False' 'true' false 
 * @returns default type-> point: (longitude: number, latitude: number) => string // wkt string
 * @returns default type-> wkt: (wktString: string) => string // valid wkt string (without self intersections using '@turf/unkink-polygon')
 * @returns default type-> geoJson: (obj: any) => string // wkt string
 * @returns default type-> orDefault: (val: any, defaultVal: any) => any
 * @returns default type-> concat: (separator: string, ...args: string[]) => string

 * @example const { $customParse } = customParseExpression({
  multAsync: async (num1: number, num2: number) => {
    await delay(1000);
    return num1*num2;
  },
  lodashGet: _.get
});
 * @example useOperators(OperatorType.EXPRESSION, { $customParse } as any); //must do to be able to use in aggregations
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
  return {
    customFunctions,
    $customParse: (obj: any, expr: { type: string, args: any[] }, options?: any) => {
      const computedValue = computeValue(obj, expr, undefined, options) as { type: string, args: any[] };
      return (customFunctions as Record<string, ParseFunc>)[computedValue.type](...(computedValue.args));
    }
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
 * @example useOperators(OperatorType.EXPRESSION, { $multAsync, $lodashGet } as any); //must do to be able to use in aggregations
 * @example {
      $project: {
        id: '$a.b',
        response: { $multAsync: ['$a.b', 5] } // this will run $multAsync(doc.a.b, 5), arguments are passed by order in an array
      }
    },...
 *
 */
export function generateCustomOperator<T extends Record<string, ParseFunc>>(customOperators: T) {
  const generatedCustomOperators: Record<keyof T, (obj: any, args: any[], options?: any) => any> = _.clone(customOperators);
  Object.keys(generatedCustomOperators).forEach(key => {
    generatedCustomOperators[key as keyof T] = (obj: any, args: any[], options?: any) => {
      const computedArgs = computeValue(obj, args, undefined, options) as any[];
      return customOperators[key](...(computedArgs));
    }
  });
  return generatedCustomOperators;
}

const MEMORY_CACHE_TIMEOUT = 600000;

/**
 * custom AsyncAggregator expression, caches request based on operation and its arguments, run it only once and returns the same response
 * @example useOperators(OperatorType.EXPRESSION, { $runOnce } as any); //must do to be able to use in aggregations
 * @example {
      $project: {
        id: '$a.b',
        response: { $runOnce: { $someAsyncCustomOperator: ['$ids'] } } // this will run someAsyncCustomOperator(ids) only once and use the response for every doc that called someAsyncCustomOperator(ids) with the same ids
      }
    },...
 *
 */
export async function $runOnce(obj: any, args: any[], options?: any) {
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
 * @example useOperators(OperatorType.EXPRESSION, { $collect } as any); //must do to be able to use in aggregations
 * @example {
      $project: {
        id: '$a.b',
        idsForBatch: { $collect: '$a.b' } // this will put in every doc an array named idsForBatch with every value of $a.b
      }
    },...
 *
 */
export async function $collect(obj: any, args: any[], options?: any) {
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
 *
 * @param pipeline an Array of pipeline operators
 * @param options An optional Options to pass the aggregator
 * @constructor
 */
export class AsyncAggregator {
  constructor(private readonly pipeline: Array<RawObject>, private readonly options?: Options) {
    useOperators(OperatorType.PIPELINE, { $addFields, $unset } as any);
    useOperators(OperatorType.EXPRESSION, { $runOnce } as any);
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