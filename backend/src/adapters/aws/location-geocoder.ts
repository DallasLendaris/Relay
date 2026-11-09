import { SearchPlaceIndexForTextCommand, type LocationClient } from '@aws-sdk/client-location';
import type { Geocoder } from '../../ports.js';

/** Bias results toward San José, [lng, lat]. */
const SAN_JOSE: [number, number] = [-121.89, 37.34];

/** Amazon Location returns the full state name in Region; the API contract wants the code. */
const STATE_CODES: Record<string, string> = {
  alabama: 'AL',
  alaska: 'AK',
  arizona: 'AZ',
  arkansas: 'AR',
  california: 'CA',
  colorado: 'CO',
  connecticut: 'CT',
  delaware: 'DE',
  'district of columbia': 'DC',
  florida: 'FL',
  georgia: 'GA',
  hawaii: 'HI',
  idaho: 'ID',
  illinois: 'IL',
  indiana: 'IN',
  iowa: 'IA',
  kansas: 'KS',
  kentucky: 'KY',
  louisiana: 'LA',
  maine: 'ME',
  maryland: 'MD',
  massachusetts: 'MA',
  michigan: 'MI',
  minnesota: 'MN',
  mississippi: 'MS',
  missouri: 'MO',
  montana: 'MT',
  nebraska: 'NE',
  nevada: 'NV',
  'new hampshire': 'NH',
  'new jersey': 'NJ',
  'new mexico': 'NM',
  'new york': 'NY',
  'north carolina': 'NC',
  'north dakota': 'ND',
  ohio: 'OH',
  oklahoma: 'OK',
  oregon: 'OR',
  pennsylvania: 'PA',
  'rhode island': 'RI',
  'south carolina': 'SC',
  'south dakota': 'SD',
  tennessee: 'TN',
  texas: 'TX',
  utah: 'UT',
  vermont: 'VT',
  virginia: 'VA',
  washington: 'WA',
  'west virginia': 'WV',
  wisconsin: 'WI',
  wyoming: 'WY',
};

/** Geocoder on Amazon Location Service (SearchPlaceIndexForText). */
export class LocationGeocoder implements Geocoder {
  constructor(
    private readonly client: Pick<LocationClient, 'send'>,
    private readonly indexName: string,
  ) {}

  async geocode(address: string) {
    const out = await this.client.send(
      new SearchPlaceIndexForTextCommand({
        IndexName: this.indexName,
        Text: address,
        FilterCountries: ['USA'],
        MaxResults: 1,
        BiasPosition: SAN_JOSE,
      }),
    );
    const place = out.Results?.[0]?.Place;
    const point = place?.Geometry?.Point;
    if (!place || !point) return null;
    const [longitude, latitude] = point; // Location order is [lng, lat]
    if (longitude === undefined || latitude === undefined) return null;

    const street = [place.AddressNumber, place.Street].filter(Boolean).join(' ');
    const shortAddress = [street, place.Municipality].filter(Boolean).join(', ');
    const region = place.Region?.trim() ?? '';
    // Accept either a full name ("California") or an already-short code ("CA").
    const state =
      STATE_CODES[region.toLowerCase()] ?? (region.length === 2 ? region.toUpperCase() : '');
    return { latitude, longitude, shortAddress: shortAddress || (place.Label ?? ''), state };
  }
}
