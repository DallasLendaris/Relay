import { LocationClient, SearchPlaceIndexForTextCommand } from '@aws-sdk/client-location';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it } from 'vitest';
import { LocationGeocoder } from '../../src/adapters/aws/location-geocoder.js';

const mock = mockClient(LocationClient);
const geocoder = new LocationGeocoder(new LocationClient({}), 'relay-index');

beforeEach(() => mock.reset());

describe('LocationGeocoder', () => {
  it('maps a California hit and sends the expected search options', async () => {
    mock.on(SearchPlaceIndexForTextCommand).resolves({
      Results: [
        {
          Place: {
            Label: '123 Main St, San Jose, CA, 95112, USA',
            AddressNumber: '123',
            Street: 'Main St',
            Municipality: 'San Jose',
            Region: 'California',
            PostalCode: '95112',
            Geometry: { Point: [-121.88, 37.33] },
          },
        },
      ],
    });
    expect(await geocoder.geocode('123 Main St San Jose')).toEqual({
      latitude: 37.33,
      longitude: -121.88,
      shortAddress: '123 Main St, San Jose',
      state: 'CA',
    });
    const input = mock.commandCalls(SearchPlaceIndexForTextCommand)[0]?.args[0].input;
    expect(input).toMatchObject({
      IndexName: 'relay-index',
      Text: '123 Main St San Jose',
      FilterCountries: ['USA'],
      MaxResults: 1,
      BiasPosition: [-121.89, 37.34],
    });
  });

  it('maps Nevada to NV', async () => {
    mock.on(SearchPlaceIndexForTextCommand).resolves({
      Results: [
        {
          Place: {
            AddressNumber: '1',
            Street: 'Fremont St',
            Municipality: 'Las Vegas',
            Region: 'Nevada',
            Geometry: { Point: [-115.14, 36.17] },
          },
        },
      ],
    });
    const hit = await geocoder.geocode('1 Fremont St Las Vegas');
    expect(hit?.state).toBe('NV');
    expect(hit?.shortAddress).toBe('1 Fremont St, Las Vegas');
  });

  it('returns null when nothing is found', async () => {
    mock.on(SearchPlaceIndexForTextCommand).resolves({ Results: [] });
    expect(await geocoder.geocode('zzzz')).toBeNull();
  });

  it('reads Location points as [lng, lat]', async () => {
    mock.on(SearchPlaceIndexForTextCommand).resolves({
      Results: [{ Place: { Region: 'California', Geometry: { Point: [-122.0, 37.0] } } }],
    });
    const hit = await geocoder.geocode('x');
    expect(hit?.longitude).toBe(-122.0);
    expect(hit?.latitude).toBe(37.0);
  });
});
