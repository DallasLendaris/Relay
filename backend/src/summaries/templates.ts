// Spanish product names follow the NWS Spanish convention: Warning = Aviso, Watch = Vigilancia, Advisory = Advertencia.
/**
 * Plain-language summary templates, keyed by NWS event.
 *
 * Writing rules (from the crisis-UX research): 3rd to 4th grade reading level, short sentences,
 * lead with the action, no jargon, never add facts that are not in the official alert.
 * Templates may only fill in {event}, {area} and {end}, and only from the alert itself.
 * {end} is the alert's `ends`, or `expires` when `ends` is missing (renderer, index.ts).
 * The first sentence is the "lead": English must contain "is in effect for" and Spanish must start
 * with "Hay ". When the alert starts more than 10 minutes after it was sent, the renderer rewrites
 * the lead ("is expected for ..., starting <time>" / "Habrá ..., a partir de <time>") so a summary
 * never claims an alert is already in effect. A test checks every template follows this shape.
 * A native-speaker review of the Spanish is still required (docs/summaries-spanish-review.md).
 * Spanish is written as natural Latin American Spanish as used in California, using "usted".
 *
 * Code keys (all upper case):
 *   CONFIRMED SAME codes (https://www.weather.gov/nwr/eventcodes, checked):
 *     FFW Flash Flood Warning, FFA Flash Flood Watch, FLW Flood Warning, FLA Flood Watch,
 *     FRW Fire Warning (state and local code), TOR Tornado Warning, TOA Tornado Watch,
 *     SVR/SVA Severe Thunderstorm Warning/Watch, EWW Extreme Wind Warning, HWW High Wind Warning,
 *     DSW Dust Storm Warning, TSW/TSA Tsunami Warning/Watch, EQW Earthquake Warning,
 *     EVI Evacuation Immediate, SPW Shelter In Place Warning, SPS Special Weather Statement.
 *     (Tsunami Advisory, Wind Advisory, the winter products, Air Quality Alert, Dense Smoke
 *     Advisory and Beach Hazards Statement have no code here and match by event name only.)
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
    text: 'Hay un aviso de inundaciones repentinas para {area}. Dura hasta {end}.',
  },
};

const flashFloodWatch: TemplateSet = {
  en: {
    whatToDo: 'Know where high ground is. Be ready to leave fast.',
    text: 'A {event} is in effect for {area}. Flooding is possible until {end}.',
  },
  es: {
    whatToDo: 'Sepa dónde hay un lugar alto. Esté listo para salir rápido.',
    text: 'Hay una vigilancia de inundaciones repentinas para {area}. Es posible que haya inundaciones hasta {end}.',
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
    whatToDo: 'Quédese en un lugar fresco. Tome agua. Revise cómo están su familia y sus vecinos.',
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

const tornadoWarning: TemplateSet = {
  en: {
    whatToDo: 'Go to a safe room now. Stay away from windows. Cover your head.',
    text: 'A {event} is in effect for {area}. It lasts until {end}.',
  },
  es: {
    whatToDo: 'Vaya ahora a un cuarto seguro. Aléjese de las ventanas. Cúbrase la cabeza.',
    text: 'Hay un aviso de tornado para {area}. Dura hasta {end}.',
  },
};

const tornadoWatch: TemplateSet = {
  en: {
    whatToDo: 'Know where your safe room is. Be ready to go there fast.',
    text: 'A {event} is in effect for {area}. Tornadoes are possible until {end}.',
  },
  es: {
    whatToDo: 'Sepa dónde está su cuarto seguro. Esté listo para ir rápido.',
    text: 'Hay una vigilancia de tornado para {area}. Es posible que haya tornados hasta {end}.',
  },
};

const tsunamiWarning: TemplateSet = {
  en: {
    whatToDo: 'Go to high ground or inland now. Do not wait. Stay away from the beach.',
    text: 'A {event} is in effect for {area}. It lasts until {end}.',
  },
  es: {
    whatToDo: 'Vaya ahora a un lugar alto o tierra adentro. No espere. Aléjese de la playa.',
    text: 'Hay un aviso de tsunami para {area}. Dura hasta {end}.',
  },
};

const tsunamiAdvisory: TemplateSet = {
  en: {
    whatToDo: 'Stay out of the water. Stay away from beaches, harbors and docks.',
    text: 'A {event} is in effect for {area}. Strong currents are possible until {end}.',
  },
  es: {
    whatToDo: 'No entre al agua. Aléjese de las playas, los puertos y los muelles.',
    text: 'Hay una advertencia de tsunami para {area}. Es posible que haya corrientes fuertes hasta {end}.',
  },
};

const tsunamiWatch: TemplateSet = {
  en: {
    whatToDo: 'Know your way to high ground. Be ready to leave fast.',
    text: 'A {event} is in effect for {area}. A tsunami is possible until {end}.',
  },
  es: {
    whatToDo: 'Sepa cómo llegar a un lugar alto. Esté listo para salir rápido.',
    text: 'Hay una vigilancia de tsunami para {area}. Es posible que haya un tsunami hasta {end}.',
  },
};

const extremeWindWarning: TemplateSet = {
  en: {
    whatToDo: 'Take shelter now in the strongest part of a building. Stay away from windows.',
    text: 'A {event} is in effect for {area}. Very strong winds last until {end}.',
  },
  es: {
    whatToDo:
      'Busque refugio ahora en la parte más fuerte de un edificio. Aléjese de las ventanas.',
    text: 'Hay un aviso de vientos extremos para {area}. Los vientos son muy fuertes hasta {end}.',
  },
};

const highWindWarning: TemplateSet = {
  en: {
    whatToDo: 'Stay inside if you can. Stay away from trees and power lines.',
    text: 'A {event} is in effect for {area}. Strong winds last until {end}.',
  },
  es: {
    whatToDo: 'Quédese adentro si puede. Aléjese de los árboles y de los cables eléctricos.',
    text: 'Hay un aviso de vientos fuertes para {area}. Los vientos fuertes duran hasta {end}.',
  },
};

const windAdvisory: TemplateSet = {
  en: {
    whatToDo: 'Hold on to loose items. Drive with care.',
    text: 'A {event} is in effect for {area}. It will be windy until {end}.',
  },
  es: {
    whatToDo: 'Asegure los objetos sueltos. Maneje con cuidado.',
    text: 'Hay una advertencia de viento para {area}. Habrá viento hasta {end}.',
  },
};

const winterStormWarning: TemplateSet = {
  en: {
    whatToDo: 'Stay home if you can. Roads may be unsafe. Keep warm.',
    text: 'A {event} is in effect for {area}. Heavy snow and ice are possible until {end}.',
  },
  es: {
    whatToDo: 'Quédese en casa si puede. Los caminos pueden ser peligrosos. Manténgase abrigado.',
    text: 'Hay un aviso de tormenta invernal para {area}. Es posible que haya mucha nieve y hielo hasta {end}.',
  },
};

const winterWeatherAdvisory: TemplateSet = {
  en: {
    whatToDo: 'Drive slowly. Watch for ice and snow on roads.',
    text: 'A {event} is in effect for {area}. Snow or ice is possible until {end}.',
  },
  es: {
    whatToDo: 'Maneje despacio. Tenga cuidado con el hielo y la nieve en los caminos.',
    text: 'Hay una advertencia de clima invernal para {area}. Es posible que haya nieve o hielo hasta {end}.',
  },
};

const dustStormWarning: TemplateSet = {
  en: {
    whatToDo: 'Pull off the road. Turn off your lights. Keep your foot off the brake.',
    text: 'A {event} is in effect for {area}. It is hard to see until {end}.',
  },
  es: {
    whatToDo: 'Salga del camino. Apague las luces. No pise el freno.',
    text: 'Hay un aviso de tormenta de polvo para {area}. Es difícil ver hasta {end}.',
  },
};

const severeThunderstormWarning: TemplateSet = {
  en: {
    whatToDo: 'Go inside a strong building. Stay away from windows.',
    text: 'A {event} is in effect for {area}. It lasts until {end}.',
  },
  es: {
    whatToDo: 'Entre a un edificio fuerte. Aléjese de las ventanas.',
    text: 'Hay un aviso de tormenta eléctrica severa para {area}. Dura hasta {end}.',
  },
};

const severeThunderstormWatch: TemplateSet = {
  en: {
    whatToDo: 'Watch the sky. Be ready to go inside fast.',
    text: 'A {event} is in effect for {area}. Severe storms are possible until {end}.',
  },
  es: {
    whatToDo: 'Vigile el cielo. Esté listo para entrar rápido.',
    text: 'Hay una vigilancia de tormenta eléctrica severa para {area}. Es posible que haya tormentas fuertes hasta {end}.',
  },
};

/** Not routed from NWS today; here in case an earthquake early warning is ever delivered. */
const earthquakeWarning: TemplateSet = {
  en: {
    whatToDo: 'Drop, cover and hold on now.',
    text: 'An {event} is in effect for {area}. Strong shaking is expected.',
  },
  es: {
    whatToDo: 'Agáchese, cúbrase y sujétese ahora.',
    text: 'Hay un aviso de terremoto para {area}. Se espera un temblor fuerte.',
  },
};

const evacuationImmediate: TemplateSet = {
  en: {
    whatToDo: 'Leave now. Follow the directions of officials.',
    text: 'An {event} is in effect for {area}. Officials say to leave now.',
  },
  es: {
    whatToDo: 'Salga ahora. Siga las instrucciones de las autoridades.',
    text: 'Hay una orden de evacuación inmediata para {area}. Las autoridades dicen que salga ahora.',
  },
};

const shelterInPlaceWarning: TemplateSet = {
  en: {
    whatToDo: 'Stay inside. Close doors and windows. Follow the directions of officials.',
    text: 'A {event} is in effect for {area}. It lasts until {end}.',
  },
  es: {
    whatToDo:
      'Quédese adentro. Cierre puertas y ventanas. Siga las instrucciones de las autoridades.',
    text: 'Hay un aviso de refugio en el lugar para {area}. Dura hasta {end}.',
  },
};

const airQualityAlert: TemplateSet = {
  en: {
    whatToDo: 'Spend less time outside. Keep windows closed.',
    text: 'An {event} is in effect for {area}. The air may be unhealthy until {end}.',
  },
  es: {
    whatToDo: 'Pase menos tiempo afuera. Mantenga las ventanas cerradas.',
    text: 'Hay una alerta de calidad del aire para {area}. El aire puede ser dañino hasta {end}.',
  },
};

const denseSmokeAdvisory: TemplateSet = {
  en: {
    whatToDo: 'Stay inside if you can. Drive slowly with your lights on.',
    text: 'A {event} is in effect for {area}. It is hard to see until {end}.',
  },
  es: {
    whatToDo: 'Quédese adentro si puede. Maneje despacio con las luces encendidas.',
    text: 'Hay una advertencia de humo denso para {area}. Es difícil ver hasta {end}.',
  },
};

const specialWeatherStatement: TemplateSet = {
  en: {
    whatToDo: 'Read the official message for details.',
    text: 'A {event} is in effect for {area}. It lasts until {end}.',
  },
  es: {
    whatToDo: 'Lea el mensaje oficial para ver los detalles.',
    text: 'Hay un comunicado meteorológico especial para {area}. Dura hasta {end}.',
  },
};

const beachHazardsStatement: TemplateSet = {
  en: {
    whatToDo: 'Be careful near the water. Stay away from rough surf.',
    text: 'A {event} is in effect for {area}. It lasts until {end}.',
  },
  es: {
    whatToDo: 'Tenga cuidado cerca del agua. Aléjese del oleaje fuerte.',
    text: 'Hay un comunicado de peligros en la playa para {area}. Dura hasta {end}.',
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
  TOR: tornadoWarning,
  TOA: tornadoWatch,
  SVR: severeThunderstormWarning,
  SVA: severeThunderstormWatch,
  EWW: extremeWindWarning,
  HWW: highWindWarning,
  DSW: dustStormWarning,
  TSW: tsunamiWarning,
  TSA: tsunamiWatch,
  EQW: earthquakeWarning,
  EVI: evacuationImmediate,
  SPW: shelterInPlaceWarning,
  SPS: specialWeatherStatement,
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
  'tornado warning': tornadoWarning,
  'tornado watch': tornadoWatch,
  'tsunami warning': tsunamiWarning,
  'tsunami advisory': tsunamiAdvisory,
  'tsunami watch': tsunamiWatch,
  'extreme wind warning': extremeWindWarning,
  'high wind warning': highWindWarning,
  'wind advisory': windAdvisory,
  'winter storm warning': winterStormWarning,
  'winter weather advisory': winterWeatherAdvisory,
  'dust storm warning': dustStormWarning,
  'severe thunderstorm warning': severeThunderstormWarning,
  'severe thunderstorm watch': severeThunderstormWatch,
  'earthquake warning': earthquakeWarning,
  'evacuation immediate': evacuationImmediate,
  'shelter in place warning': shelterInPlaceWarning,
  'air quality alert': airQualityAlert,
  'dense smoke advisory': denseSmokeAdvisory,
  'special weather statement': specialWeatherStatement,
  'beach hazards statement': beachHazardsStatement,
};
