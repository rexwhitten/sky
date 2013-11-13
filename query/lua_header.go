package query

const luaHeader = `
-- SKY GENERATED CODE BEGIN --
local ffi = require('ffi')
ffi.cdef([[

typedef struct sky_string_t {
  int32_t length;
  char *data;
} sky_string_t;

typedef struct {
  {{range .}}{{structdef .}}
  {{end}}
  int64_t _ts;
  uint32_t _timestamp;
} sky_lua_event_t;

typedef struct sky_cursor_t {
  sky_lua_event_t *event;
  sky_lua_event_t *next;
  uint32_t next_timestamp;
  uint32_t max_timestamp;
  uint32_t session_idle_in_sec;
} sky_cursor_t;

int sky_cursor_set_data_sz(sky_cursor_t *cursor, uint32_t sz);
int sky_cursor_set_timestamp_offset(sky_cursor_t *cursor, uint32_t offset);
int sky_cursor_set_ts_offset(sky_cursor_t *cursor, uint32_t offset);
int sky_cursor_set_property(sky_cursor_t *cursor, int64_t property_id, uint32_t offset, uint32_t sz, const char *data_type);
int sky_cursor_set_max_timestamp(sky_cursor_t *cursor, uint32_t timestamp);

bool sky_cursor_has_next_object(sky_cursor_t *);
bool sky_cursor_next_object(sky_cursor_t *);
bool sky_cursor_eof(sky_cursor_t *);
bool sky_cursor_eos(sky_cursor_t *);
bool sky_lua_cursor_next_event(sky_cursor_t *);
bool sky_lua_cursor_next_session(sky_cursor_t *);
bool sky_cursor_set_session_idle(sky_cursor_t *, uint32_t);
]])
ffi.metatype('sky_cursor_t', {
  __index = {
    set_data_sz = function(cursor, sz) return ffi.C.sky_cursor_set_data_sz(cursor, sz) end,
    set_timestamp_offset = function(cursor, offset) return ffi.C.sky_cursor_set_timestamp_offset(cursor, offset) end,
    set_ts_offset = function(cursor, offset) return ffi.C.sky_cursor_set_ts_offset(cursor, offset) end,
    set_action_id_offset = function(cursor, offset) return ffi.C.sky_cursor_set_action_id_offset(cursor, offset) end,
    set_property = function(cursor, property_id, offset, sz, data_type) return ffi.C.sky_cursor_set_property(cursor, property_id, offset, sz, data_type) end,
    set_max_timestamp = function(cursor, timestamp) return ffi.C.sky_cursor_set_max_timestamp(cursor, timestamp) end,

    hasNextObject = function(cursor) return ffi.C.sky_cursor_has_next_object(cursor) end,
    nextObject = function(cursor) return ffi.C.sky_cursor_next_object(cursor) end,
    eof = function(cursor) return ffi.C.sky_cursor_eof(cursor) end,
    eos = function(cursor) return ffi.C.sky_cursor_eos(cursor) end,
    next = function(cursor) return ffi.C.sky_lua_cursor_next_event(cursor) end,
    next_session = function(cursor) return ffi.C.sky_lua_cursor_next_session(cursor) end,
    set_session_idle = function(cursor, seconds) return ffi.C.sky_cursor_set_session_idle(cursor, seconds) end,
  }
})
ffi.metatype('sky_lua_event_t', {
  __index = {
  timestamp = function(event) return event._timestamp end,
  {{range .}}{{metatypedef .}}
  {{end}}
  }
})

function sky_init_cursor(_cursor)
  cursor = ffi.cast('sky_cursor_t*', _cursor)
  {{range .}}{{initdescriptor .}}
  {{end}}
  cursor:set_timestamp_offset(ffi.offsetof('sky_lua_event_t', '_timestamp'))
  cursor:set_ts_offset(ffi.offsetof('sky_lua_event_t', '_ts'))
  cursor:set_data_sz(ffi.sizeof('sky_lua_event_t'))
end

-- A reference to the error thrown when exiting an object query.
exit_error = {}


----------------------------------------------------------------------
--
-- Histogram Functions
--
----------------------------------------------------------------------

-- Checks if a value is a histogram.
function sky_is_histogram(histogram)
   return (histogram ~= nil and histogram.__histogram__ == true)
end

-- Creates a new histogram object.
function sky_histogram_new()
  return {__histogram__=true, min=0, max=0, count=0, width=0, bins={}, values={}}
end

-- Converts a histogram with values into a set of bins and clears the values.
function sky_histogram_finalize(histogram)
  table.sort(histogram.values)
  histogram.min, histogram.max = histogram.values[1], histogram.values[#histogram.values]

  histogram.count = math.ceil(math.sqrt(#histogram.values))
  if histogram.count == 0 then histogram.count = 1 end

  histogram.width = (histogram.max - histogram.min) / histogram.count
  if histogram.width == 0 then histogram.width = 1 end

  for i=0,histogram.count-1 do
    histogram.bins[i] = 0
  end

  histogram.values = nil
end

-- Inserts a value into an existing histogram.
function sky_histogram_insert(histogram, value)
  if histogram == nil or histogram.count == 0 then return end
  index = math.floor((value-histogram.min)/histogram.width)
  index = math.min(math.max(index, 0), histogram.count-1)
  histogram.bins[index] = histogram.bins[index] + 1
end

-- Merges one histogram into another. It's assumed that both histograms share
-- the same structure and bin count.
function sky_histogram_merge(a, b)
  if a == nil then
    a = b
  elseif b ~= nil then
    for i=0,a.count-1 do
      a.bins[i] = a.bins[i] + b.bins[i]
    end
  end
  return a
end


----------------------------------------------------------------------
--
-- Average Functions
--
----------------------------------------------------------------------

-- Checks if a value is an average.
function sky_is_average(average)
   return (average ~= nil and average.__average__ == true)
end

-- Creates a new average object.
function sky_average_new()
  return {__average__=true, count=0, sum=0}
end

-- Inserts a value into an existing average.
function sky_average_insert(average, value)
  average.count = average.count + 1
  average.sum   = average.sum + value
end

-- Merges one average into another.
function sky_average_merge(a, b)
  -- takes two averages
  if a == nil then
    a = b
  elseif b ~= nil then
    a.count = a.count + b.count
    a.sum = a.sum + b.sum
    a.avg = a.sum / a.count
  end
  return a
end


----------------------------------------------------------------------
--
-- Distinct Functions
--
----------------------------------------------------------------------

-- Checks if a value is a distinct.
function sky_is_distinct(distinct)
   return (distinct ~= nil and distinct.__distinct__ == true)
end

-- Creates a new distinct object.
function sky_distinct_new()
  return {__distinct__=true, values={}}
end

-- Inserts a value into an existing distinct.
function sky_distinct_insert(distinct, value)
  distinct.values[value] = 1
end

-- Merges one distinct into another.
function sky_distinct_merge(a, b)
  -- takes two distincts
  if a == nil then
    a = b
  elseif b ~= nil then
    for k,v in pairs(b.values) do sky_distinct_insert(a, k) end

    local count = 0
    for k,v in pairs(a.values) do count = count + 1 end
    a.distinct = count
  end
  return a
end


----------------------------------------------------------------------
--
-- Utility Functions
--
----------------------------------------------------------------------

-- Finalizes a data structure so that it can be used to seed the aggregation.
function sky_finalize(value)
  if type(value) ~= "table" then return nil end

  if sky_is_histogram(value) then
    sky_histogram_finalize(value)
  else
    for k,v in pairs(value) do
      value[k] = sky_finalize(v)
    end
  end

  return value
end


----------------------------------------------------------------------
--
-- Entry Functions
--
----------------------------------------------------------------------

-- Initializes the data structure used by all servlets. This is
-- necessary for histograms where the number and size of of bins
-- needs to be calculated.
function sky_initialize(_cursor)
  cursor = ffi.cast('sky_cursor_t*', _cursor)
  index = 0
  data = {}
  while cursor:nextObject() do
    initialize(cursor, data)
    index = index + 1
    if index >= 1000 then break end
  end
  sky_finalize(data)
  return data
end

-- Loops over every event for every object. This is run once per
-- servlet.
function sky_aggregate(_cursor, data)
  cursor = ffi.cast('sky_cursor_t*', _cursor)
  if data == nil then data = {} end
  while cursor:nextObject() do
    aggregate(cursor, data)
    -- status, err = pcall(function() aggregate(cursor, data) end)
    -- if not status and err ~= exit_error then error(err) end
  end
  return data
end

-- Loops over the results from the aggregate function and combines
-- them into a single table.
function sky_merge(results, data)
  if data ~= nil then
    merge(results, data)
  end
  return results
end
-- SKY GENERATED CODE END --
`
