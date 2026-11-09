import { MemoryCellIndex } from '../../../src/scale/cell-index/index.js';
import { cellIndexContract } from './contract.js';

cellIndexContract('MemoryCellIndex', () => new MemoryCellIndex());
