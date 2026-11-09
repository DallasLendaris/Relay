// Spanish product names follow the NWS Spanish convention: Warning = Aviso, Watch = Vigilancia, Advisory = Advertencia.
/**
 * Plain-language summary templates, keyed by NWS event.
 *
 * Writing rules (from the crisis-UX research): 3rd to 4th grade reading level, short sentences,
 * lead with the action, no jargon, never add facts that are not in the official alert.
 * Templates may only fill in {event}, {area} and {end}, and only from the alert itself.
 * Spanish is written as natural Latin American Spanish as used in California, using "usted".
 *
 * Code keys (all upper case):
 *   CONFIRMED SAME codes (https://www.weather.gov/nwr/eventcodes, checked):
 *     FFW Flash Flood Warning, FFA Flash Flood Watch, FLW Flood Warning, FLA Flood Watch,
 *     FRW Fire Warning (state and local code).
 *   NOT SAME codes. SAME has no codes for Red Flag, Fire Weather Watch, Flood Advisory or any heat
 *   product, and FLS is "Flood Statement", not an advisory. The keys below are our best guess at
 *   what Alert.eventCode may carry for those products, and we have not verified them against
 *   live NWS data. The event name key is the reliable path for them:
 *     RFW, FWW, FWA  fire weather (VTEC phenomenon FW; significance W = Red Flag Warning, A = Fire Weather Watch)
 *     EHW, EHA, HTY  heat (VTEC phenomenon EH = Extreme Heat, HT = Heat; significance W/A/Y)
 *     FAY            flood advisory (VTEC phenomena FA/FL with significance Y)
 * Name keys are lower case and also cover the older "Excessive Heat" names.
 */

export type Language = 'en' | 'es';

export interface SummaryTemplate {
  /** 1 to 3 short imperative steps. */
  whatToDo: string;
  /** 1 to 2 sentences: what is happening, where ({area}) and until when ({end}). May use {event}. */
  text: string;
}

export type TemplateSet = Record<Language, SummaryTemplate>;

const flashFloodWarning: TemplateSet = {
  en: {
    whatToDo: 'Move to high ground now. Do not walk or drive through flood water.',
    text: 'A {event} is in effect for {area}. It lasts until {end}.',
  },
  es: {
    whatToDo: 'Suba a un lugar alto ahora. No camine ni maneje por el agua de la inundación.',
    text: 'Hay un aviso de inundación repentina para {area}. Dura hasta {end}.',
  },
};

const flashFloodWatch: TemplateSet = {
  en: {
    whatToDo: 'Know where high ground is. Be ready to leave fast.',
    text: 'A {event} is in effect for {area}. Flooding is possible until {end}.',
  },
  es: {
    whatToDo: 'Sepa dónde hay un lugar alto. Esté listo para salir rápido.',
    text: 'Hay una vigilancia de inundación repentina para {area}. Es posible que haya inundaciones hasta {end}.',
  },
};

const floodWarning: TemplateSet = {
  en: {
    whatToDo: 'Stay away from flood water. Move to higher ground if you are told to.',
    text: 'A {event} is in effect for {area}. It lasts until {end}.',
  },
  es: {
    whatToDo: 'Aléjese del agua de la inundación. Suba a un lugar alto si se lo indican.',
    text: 'Hay un aviso de inundación para {area}. Dura hasta {end}.',
  },
};

const floodWatch: TemplateSet = {
  en: {
    whatToDo: 'Check your plan. Be ready to move if the water rises.',
    text: 'A {event} is in effect for {area}. Flooding is possible until {end}.',
  },
  es: {
    whatToDo: 'Repase su plan. Esté listo para irse si sube el agua.',
    text: 'Hay una vigilancia de inundación para {area}. Es posible que haya inundaciones hasta {end}.',
  },
};

const floodAdvisory: TemplateSet = {
  en: {
    whatToDo: 'Drive slowly. Stay out of water on roads.',
    text: 'A {event} is in effect for {area}. It lasts until {end}.',
  },
  es: {
    whatToDo: 'Maneje despacio. No pase por el agua en las calles.',
    text: 'Hay una advertencia de inundación para {area}. Dura hasta {end}.',
  },
};

const redFlagWarning: TemplateSet = {
  en: {
    whatToDo: 'Do not start fires or burn trash. Be ready to leave fast.',
    text: 'A {event} is in effect for {area}. Fires can start and spread fast until {end}.',
  },
  es: {
    whatToDo: 'No haga fuego ni queme basura. Esté listo para salir rápido.',
    text: 'Hay un aviso de bandera roja para {area}. Los incendios pueden empezar y crecer rápido hasta {end}.',
  },
};

const fireWeatherWatch: TemplateSet = {
  en: {
    whatToDo: 'Do not start fires. Get your go bag ready.',
    text: 'A {event} is in effect for {area}. Fire danger may rise until {end}.',
  },
  es: {
    whatToDo: 'No haga fuego. Prepare su mochila de emergencia.',
    text: 'Hay una vigilancia de clima de incendios para {area}. El peligro de incendio puede subir hasta {end}.',
  },
};

const fireWarning: TemplateSet = {
  en: {
    whatToDo: 'Leave if officials tell you to. Follow their directions.',
    text: 'A {event} is in effect for {area}. It lasts until {end}.',
  },
  es: {
    whatToDo: 'Salga si las autoridades se lo piden. Siga sus instrucciones.',
    text: 'Hay un aviso de incendio para {area}. Dura hasta {end}.',
  },
};

const extremeHeatWarning: TemplateSet = {
  en: {
    whatToDo: 'Stay in a cool place. Drink water. Check on family and neighbors.',
    text: 'A {event} is in effect for {area}. The heat is dangerous until {end}.',
  },
  es: {
    whatToDo: 'Quédese en un lugar fresco. Tome agua. Fíjese cómo están su familia y sus vecinos.',
    text: 'Hay un aviso de calor extremo para {area}. El calor es peligroso hasta {end}.',
  },
};

const extremeHeatWatch: TemplateSet = {
  en: {
    whatToDo: 'Plan a cool place to stay. Have water ready.',
    text: 'A {event} is in effect for {area}. Dangerous heat is possible until {end}.',
  },
  es: {
    whatToDo: 'Busque un lugar fresco donde quedarse. Tenga agua lista.',
    text: 'Hay una vigilancia de calor extremo para {area}. Es posible que haya calor peligroso hasta {end}.',
  },
};

const heatAdvisory: TemplateSet = {
  en: {
    whatToDo: 'Drink water. Rest in the shade or inside. Never leave a child or pet in a car.',
    text: 'A {event} is in effect for {area}. It will be hot until {end}.',
  },
  es: {
    whatToDo:
      'Tome agua. Descanse a la sombra o adentro. Nunca deje a un niño o una mascota en el carro.',
    text: 'Hay una advertencia de calor para {area}. Va a hacer calor hasta {end}.',
  },
};

/** Keyed by upper-case event code. See the header comment for which codes are confirmed. */
export const TEMPLATES_BY_CODE: Record<string, TemplateSet> = {
  FFW: flashFloodWarning,
  FFA: flashFloodWatch,
  FLW: floodWarning,
  FLA: floodWatch,
  FAY: floodAdvisory,
  RFW: redFlagWarning,
  FWW: redFlagWarning,
  FWA: fireWeatherWatch,
  FRW: fireWarning,
  EHW: extremeHeatWarning,
  EHA: extremeHeatWatch,
  HTY: heatAdvisory,
};

/** Fallback keyed by lower-case event name, used when eventCode is null or unknown. */
export const TEMPLATES_BY_EVENT: Record<string, TemplateSet> = {
  'flash flood warning': flashFloodWarning,
  'flash flood watch': flashFloodWatch,
  'flood warning': floodWarning,
  'flood watch': floodWatch,
  'flood advisory': floodAdvisory,
  'red flag warning': redFlagWarning,
  'fire weather watch': fireWeatherWatch,
  'fire warning': fireWarning,
  'extreme heat warning': extremeHeatWarning,
  'excessive heat warning': extremeHeatWarning,
  'extreme heat watch': extremeHeatWatch,
  'excessive heat watch': extremeHeatWatch,
  'heat advisory': heatAdvisory,
};
