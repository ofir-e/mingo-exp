import { deasyncObj } from "deasync-obj";
import { Aggregator } from "mingo";
import {
  Options,
} from "mingo/core";
import { RawObject } from "mingo/types";

/**
 * Provides functionality for the mongoDB aggregation pipeline
 *
 * @param pipeline an Array of pipeline operators
 * @param options An optional Options to pass the aggregator
 * @constructor
 */
export class AsyncAggregator {
  constructor(private readonly pipeline: Array<RawObject>, private readonly options?: Options) {}
  
  async run(collection: Array<RawObject>): Promise<Array<RawObject>> {

    const aggregators = this.pipeline.map(pipe => new Aggregator([pipe], this.options));
    for (const agg of aggregators) {
      collection = agg.run(collection);
      await deasyncObj(collection);
    }
    return collection;
  }
}