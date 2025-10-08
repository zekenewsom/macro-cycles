export type Meta = { data_vintage: string; config_version: string; git_hash: string };

export type CompositePoint = {
  date: string; score: number; z: number; regime: string; confidence: number;
};

export type PillarCard = {
  pillar: string; z: number; momentum_3m: number; diffusion: number;
  components: { id: string; label: string; z: number; delta: number; source: string }[];
};

export type CompositeResponse = { series: CompositePoint[]; meta: Meta };
export type PillarsResponse   = { pillars: PillarCard[]; meta: Meta };
export type MarketRegimesResp = { assets: any[]; meta: Meta };
export type MoversResponse    = { gainers: any[]; losers: any[]; meta: Meta };

