/** One queue configured in the gateway. */
export interface QueueModel {
  /** Unique queue name. */
  name: string;
  /** Optional description; empty when the gateway provides none. */
  description: string;
}
