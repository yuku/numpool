CREATE TABLE numpools (
  id VARCHAR(100) PRIMARY KEY,
  max_resources_count INT NOT NULL CHECK (max_resources_count BETWEEN 1 AND 64),
  resource_usage_status BIT(64) NOT NULL DEFAULT 0::BIT(64),
  wait_queue VARCHAR(100)[] NOT NULL DEFAULT '{}',
  metadata JSONB
);

COMMENT ON TABLE numpools IS 'Numpool represents an abstract pool of resources.';

COMMENT ON COLUMN numpools.id IS 'ID is a unique identifier for the numpool.
It is used to identify the numpool in the database.';

COMMENT ON COLUMN numpools.max_resources_count IS 'MaxResourcesCount is the maximum number of resources that can be allocated.
It is used to limit the number of resources that can be created.
The value must be between 1 and 64, inclusive.';

COMMENT ON COLUMN numpools.resource_usage_status IS 'ResourceUsageStatus is a bitmask representing the usage of resources.
Each bit corresponds to a resource, where 1 means the resource is in use and 0 means it is free.
The length of the bitmask is equal to MaxResourcesCount.
For example, if MaxResourcesCount is 8, then ResourceUsageStatus can be a value from 0 to 64, where each bit represents the usage of a resource.';

COMMENT ON COLUMN numpools.wait_queue IS 'WaitQueue is a list of client ids waiting for the numpool.';

COMMENT ON COLUMN numpools.metadata IS 'Metadata is a JSONB field that can store additional information about the numpool.
It can be used to store any additional data that is relevant to the numpool, such as
configuration settings, descriptions, or other metadata that does not fit into the other fields.';
