import { booleanPointInPolygon } from '@turf/boolean-point-in-polygon';
import type { Feature, Polygon } from 'geojson';

/**
 * DEMO ONLY: a very rough California outline (about 25 vertices, [lng, lat]).
 * It is good enough to reject Nevada, Oregon and Arizona points in a demo, but
 * it is NOT survey-accurate: points within a few km of the border may be wrong.
 * Replace with the Census state boundary before a real pilot.
 */
export const CALIFORNIA_BOUNDARY: Feature<Polygon> = {
  type: 'Feature',
  properties: { name: 'California (simplified)' },
  geometry: {
    type: 'Polygon',
    coordinates: [
      [
        [-124.4, 42.0],
        [-120.0, 42.0],
        [-120.0, 39.0],
        [-114.63, 35.0],
        [-114.57, 34.3],
        [-114.43, 34.1],
        [-114.72, 33.4],
        [-114.5, 33.0],
        [-114.72, 32.72],
        [-117.12, 32.53],
        [-117.3, 32.8],
        [-118.5, 33.4],
        [-119.5, 33.95],
        [-120.65, 34.45],
        [-121.0, 35.1],
        [-122.0, 36.5],
        [-122.5, 37.5],
        [-123.0, 37.9],
        [-123.8, 39.0],
        [-124.1, 40.2],
        [-124.4, 40.4],
        [-124.2, 41.5],
        [-124.4, 42.0],
      ],
    ],
  },
};

export function isInCalifornia(latitude: number, longitude: number): boolean {
  return booleanPointInPolygon([longitude, latitude], CALIFORNIA_BOUNDARY);
}
