import { computeValue, Options, useOperators, OperatorType } from 'mingo/core';
import _ from 'lodash';
import { Point, Geometry, GeometryCollection } from 'wkx';
import unkinkPolygon from '@turf/unkink-polygon';
import dayjs from 'dayjs';
import { RawObject } from 'mingo/types';
import { Aggregator } from 'mingo';
import { deasyncObj } from 'deasync-obj';
import { v4 as uuidv4 } from 'uuid';
import { $addFields, $unset } from 'mingo/operators/pipeline';
import Cache from 'memory-cache';
import hash from 'object-hash';
import AsyncLock from 'async-lock';
const lock = new AsyncLock();

type ParseFunc = (...args: any[]) => any;

const defaultCustomFunctions = {
  date: (val: any) => dayjs(val).toISOString(),
  number: (val: any) => _.toNumber(val),
  text: (val: any) => _.toString(val).trim(),
  boolean: (val: string | boolean) => {
    if (val?.toString()?.toLowerCase() === 'true') return true;
    if (val?.toString()?.toLowerCase() === 'false') return false;
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
  geoJson: (type: string, coordinates: any[]) => Geometry.parseGeoJSON({ type, coordinates }).toWkt(),
  orDefault: (val: any, defaultVal: any) => (_.isEmpty(val) ? defaultVal : val),
  concat: (separator: string, ...args: string[]) => _.compact(args).join(separator)
}

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