package io.deephaven.client.impl

import org.apache.arrow.flight.FlightDescriptor

object FlightDescriptorHelper {
  def descriptor(hasPathId: HasPathId): FlightDescriptor =
    FlightDescriptor.path(hasPathId.pathId().path())
}
