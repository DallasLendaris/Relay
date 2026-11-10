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
      shortAddress: 'Main St, San Jose',
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
    expect(hit?.shortAddress).toBe('Fremont St, Las Vegas');
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

  describe('PRIV-5: never returns a house number', () => {
    const cases: Array<{ name: string; place: Record<string, unknown>; expected: string }> = [
      {
        name: '1600 Amphitheatre Pkwy',
        place: {
          Label: '1600 Amphitheatre Pkwy, Mountain View, CA, 94043, USA',
          AddressNumber: '1600',
          Street: 'Amphitheatre Pkwy',
          Municipality: 'Mountain View',
        },
        expected: 'Amphitheatre Pkwy, Mountain View',
      },
      {
        name: '12-B Main St',
        place: { AddressNumber: '12-B', Street: 'Main St', Municipality: 'San Jose' },
        expected: 'Main St, San Jose',
      },
      {
        name: 'One Market St (number inside Street)',
        place: { Street: 'One Market St', Municipality: 'San Francisco' },
        expected: 'Market St, San Francisco',
      },
      {
        name: 'number left in Street',
        place: { Street: '12-B Main St', Municipality: 'San Jose' },
        expected: 'Main St, San Jose',
      },
      {
        name: 'unit-suffixed number left in Street',
        place: { Street: '221B Baker St', Municipality: 'Oakland' },
        expected: 'Baker St, Oakland',
      },
      {
        name: 'ordinal street name is kept',
        place: { AddressNumber: '90', Street: '1st St', Municipality: 'San Jose' },
        expected: '1st St, San Jose',
      },
      {
        name: 'no street: locality only, never the Label',
        place: {
          Label: '1600 Amphitheatre Pkwy, Mountain View, CA, USA',
          Municipality: 'Mountain View',
        },
        expected: 'Mountain View',
      },
      {
        name: 'nothing usable: empty, never the Label',
        place: { Label: '500 Oak Ave, Fresno, CA, USA' },
        expected: '',
      },
    ];
    for (const c of cases) {
      it(c.name, async () => {
        mock.on(SearchPlaceIndexForTextCommand).resolves({
          Results: [
            {
              Place: { Region: 'California', Geometry: { Point: [-121.9, 37.3] }, ...c.place },
            },
          ],
        });
        const hit = await geocoder.geocode('x');
        expect(hit?.shortAddress).toBe(c.expected);
        expect(hit?.shortAddress).not.toMatch(/^\s*\d+(?:-\w+)?[\s,]/);
        expect(hit?.shortAddress).not.toMatch(/^(one|two|three)\b/i);
      });
    }
  });
});
