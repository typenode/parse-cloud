export function trigger(triggers: Triggers) {
  return cloud.registerTriggers(triggers);
}
export function resolve<T = any, U = Parse.Attributes>(resolver: Resolvers<T>) {
  return cloud.registerResolvers<T, U>(resolver);
}
export function schema<T>(schema: ISchema): Model<T> {
  return cloud.registerSchema(schema);
}
export function define(func: (request: Parse.Cloud.FunctionRequest) => any) {
  return cloud.registerFunction(func);
}

export function setup(hooks: Setup) {
  cloud.registerHooks(hooks);
}

export class Cloud {
  #schemas: Schemas = {};
  #hooks = new Set<Setup>();
  readonly config: CloudConfig;
  readonly logger = new Logger();

  public registerTriggers(triggers: Triggers) {
    Object.keys(triggers).forEach(className => {
      Object.keys(triggers[ className ]).forEach(triggerName => {
        const cloudFunction = triggers[ className ][ triggerName ];
        Parse.Cloud[ triggerName ](className, cloudFunction);
      });
    });
  }
  public registerResolvers<T, U>(resolvers: Resolvers<T>) {
    Object.keys(resolvers).forEach(typename => {
      const fields = resolvers[ typename ];
      Object.keys(fields).forEach(resolverName => {
        Parse.Cloud.define(resolverName, (request) => {
          const _source = request[ "_source" ];
          const _info = request[ "_info" ];
          const parentName = _info.parentType.name;
          if (parentName === typename) {
            return fields[ resolverName ](_source, request.params, request.user, _info);
          }
          throw new Parse.Error(-1, `Invalid resolver ${typename}:${resolverName}`);
        });
      });
    });
  }
  public registerSchema<T extends Parse.Attributes>(schema: ISchema): Model<T> {
    const cloudSchema = this.#schemas[ schema.className ] = new CloudSchema(schema.className);
    if (schema.fields) {
      Object.keys(schema.fields).forEach(fieldName => {
        const { type, ...options } = schema.fields[ fieldName ];
        cloudSchema.addField(fieldName, type, options);
        cloudSchema[ "_fields" ][ fieldName ] = schema.fields[ fieldName ];
      });
    }
    if (schema.classLevelPermissions) {
      cloudSchema.setCLP(schema.classLevelPermissions);
    }
    if (schema.indexes) {
      Object.keys(schema.indexes).forEach(name => {
        cloudSchema.addIndex(name, schema.indexes[ name ]);
      });
    }
    return Parse.Object.extend(schema.className);
  }
  public registerFunction(func: (request: Parse.Cloud.FunctionRequest) => any) {
    return Parse.Cloud.define(func.name, func);
  }
  public registerHooks(hooks: Setup) {
    this.#hooks.add(hooks);
  }
  private onStart() {
    this.#hooks.forEach(hook => {
      hook.onStart(this);
    });
  }
  private async runMigrations() {
    await this.resetSchemas();
    await this.syncSchemas();
  }
  private async syncSchemas() {
    if (this.config.sync) {
      const schemas: ISchema[] = await CloudSchema.all() as any;
      const localSchemas = this.#schemas;
      const remoteSchemas = schemas.reduce((obj, curr) => {
        obj[ curr.className ] = curr;
        return obj;
      }, {});
      const localClassNames = Object.keys(localSchemas);
      const remoteClassNames = Object.keys(remoteSchemas);
      const { add, remove, update } = this.getActions(localClassNames, remoteClassNames);
      await Promise.all(add.map(async className => {
        await localSchemas[ className ].save();
        this.logger.debug(`CREATED: ${className} class.`);
      }));
      await Promise.all(remove.map(async className => {
        await new CloudSchema(className).purge();
        await new CloudSchema(className).delete();
        this.logger.debug(`DELETED: ${className} class.`);
      }));
      await Promise.all(update.map(async className => {
        const localFields = localSchemas[ className ][ "_fields" ];
        const remoteFields = remoteSchemas[ className ].fields;
        const localIndexes = remoteSchemas[ className ][ "_indexes" ];
        const remoteIndexes = remoteSchemas[ className ].indexes;
        await this.syncFields(className, localFields, remoteFields);
        await this.syncIndexes(className, localIndexes, remoteIndexes);
      }));
      if (add.length) {
        this.logger.debug(`CREATED: ${add.join(",")} classes`);
      }
      if (remove.length) {
        this.logger.debug(`DELETED: ${add.join(",")} classes`);
      }
    }
  }
  private async syncFields(className: string, localFields: ISchema["fields"] = {}, remoteFields: ISchema["fields"] = {}) {
    const local = Object.keys(localFields);
    const remote = Object.keys(remoteFields);
    const { add, remove, update } = this.getActions(local, remote.filter(n => !["objectId", "createdAt", "updatedAt", "ACL"].includes(n)));
    let schema = new CloudSchema(className);
    add.map(fieldName => {
      const { type, ...options } = localFields[ fieldName ];
      schema.addField(fieldName, type, options);
      this.logger.debug(`CREATE: ${fieldName} field.`);
    });
    remove.map(fieldName => {
      schema.deleteField(fieldName);
      this.logger.debug(`DELETE: ${fieldName} field.`);
    });
    await schema.update();
    await Promise.all(update.map(async fieldName => {
      const remoteField = remoteFields[ fieldName ];
      const localField = localFields[ fieldName ];
      if (!deepEqual(remoteField, localField)) {
        const schema = new CloudSchema(className);
        if (remoteField) {
          schema.deleteField(fieldName);
          await schema.update();
          this.logger.debug(`DELETED: ${fieldName} field.`);
        }
        const { type, ...options } = localField;
        schema.addField(fieldName, type, options);
        await schema.update();
        this.logger.debug(`CREATED: ${fieldName} field.`);
      }
    }));
  }
  private async syncIndexes(className: string, localIndexes: ISchema["indexes"] = {}, remoteIndexes: ISchema["indexes"] = {}) {
    const local = Object.keys(localIndexes);
    const remote = Object.keys(remoteIndexes);
    const { add, remove, update } = this.getActions(local, remote);
    let schema = new CloudSchema(className);
    add.map(indexName => {
      const index = localIndexes[ indexName ];
      schema.addIndex(indexName, index);
      this.logger.debug(`CREATE: ${indexName} index.`);
    });
    remove.map(indexName => {
      schema.deleteIndex(indexName);
      this.logger.debug(`DELETE: ${indexName} index.`);
    });
    await schema.update();
    await Promise.all(update.map(async indexName => {
      const remoteIndex = remoteIndexes[ indexName ];
      const localIndex = localIndexes[ indexName ];
      if (!deepEqual(remoteIndex, localIndex)) {
        const schema = new CloudSchema(className);
        if (remoteIndex) {
          schema.deleteIndex(indexName);
          await schema.update();
          this.logger.debug(`DELETED: ${indexName} index.`);
        }
        const index = localIndexes[ indexName ];
        schema.addIndex(indexName, index);
        await schema.update();
        this.logger.debug(`CREATED: ${indexName} index.`);
      }
    }));
  }
  private async resetSchemas() {
    if (this.config.reset) {
      this.logger.debug("truncating database...");
      let schemas = await CloudSchema.all();
      await Promise.all(schemas.map(json => new CloudSchema(json[ "className" ]).purge()));
      await Promise.all(Object.keys(this.#schemas).map(className => new CloudSchema(className).delete()));
      this.logger.debug("successfully truncated.");
    }
  }
  protected getActions(local: string[], remote: string[]) {
    const merged = new Set<string>([...local, ...remote]);
    const add = [];
    const remove = [];
    const update = [];
    merged.forEach(item => {
      if (remote.includes(item) && !local.includes(item)) {
        if (!item.startsWith("_")) {
          remove.push(item);
        }
      } else if (!remote.includes(item) && local.includes(item)) {
        add.push(item);
      } else if (remote.includes(item) && local.includes(item)) {
        update.push(item);
      }
    });
    return { add, remove, update };
  }
  public async setup(config: CloudConfig) {
    try {
      Object.assign(this, { config });
      await config.module;
      await this.runMigrations();
      this.onStart();
      this.logger.debug("cloud has been initialized.");
    } catch (e) {
      this.logger.error("Unable to setup cloud.", e);
    }
  }
}

export interface Trigger {
  afterDelete?<T extends Parse.Object = Parse.Object>(request: Parse.Cloud.AfterDeleteRequest<T>): Promise<void> | void;
  afterSave?<T extends Parse.Object = Parse.Object>(request: Parse.Cloud.AfterSaveRequest<T>): Promise<void> | void;
  beforeDelete?<T extends Parse.Object = Parse.Object>(request: Parse.Cloud.BeforeDeleteRequest<T>): Promise<void> | void;
  beforeSave?<T extends Parse.Object = Parse.Object>(request: Parse.Cloud.BeforeSaveRequest<T>): Promise<void> | void;
  beforeFind?<T extends Parse.Object = Parse.Object>(request: Parse.Cloud.BeforeFindRequest<T>): Promise<Parse.Query<T>> | Promise<void> | Parse.Query<T> | void;
  afterFind?<T extends Parse.Object = Parse.Object>(request: Parse.Cloud.AfterFindRequest<T>): any;
}
export interface Triggers {
  [ k: string ]: Trigger
}
export interface Model<T> extends Parse.ObjectStatic {
  new(attributes: T, options?: any): Parse.Object<T>;
}

export interface IBaseField {
  type: Parse.Schema.TYPE
  required?: boolean;
  defaultValue?: unknown
}
export interface IStringField extends IBaseField {
  type: "String";
  defaultValue?: string;
}
export interface INumberField extends IBaseField {
  type: "Number";
  defaultValue?: number;
}
export interface IBooleanField extends IBaseField {
  type: "Boolean";
  defaultValue?: boolean;
}
export interface IFileField extends IBaseField {
  type: "File";
  defaultValue?: File;
}
export interface IPolygonField extends IBaseField {
  type: "Polygon";
  defaultValue?: boolean;
}
export interface IDateField extends IBaseField {
  type: "Date";
  isTime?: boolean;
  defaultValue?: string | Date;
}
export interface IRelationField extends IBaseField {
  type: "Relation";
  targetClass: string;
}
export interface IPointerField extends IBaseField {
  type: "Pointer";
  targetClass: string;
}
export interface IObjectField extends IBaseField {
  type: "Object";
  targetClass?: string;
  schema?: INestedSchema
}
export interface IGeoPointField extends IBaseField {
  type: "GeoPoint";
}
export interface IArrayField extends IBaseField {
  type: "Array";
  defaultValue?: any[];
  targetClass?: string;
  schema?: INestedSchema
}

export type IField =
  IBooleanField |
  IStringField |
  IFileField |
  INumberField |
  IDateField |
  IObjectField |
  IGeoPointField |
  IPolygonField |
  IArrayField |
  IPointerField |
  IRelationField;

export interface ISchema {
  className: string,
  fields?: {
    [ key: string ]: IField
  }
  indexes?: {
    [ key: string ]: any
  }
  classLevelPermissions?: Parse.Schema.CLP
}
export interface INestedSchema {
  className: string
  fields: {
    [ key: string ]: IField
  }
}
export interface Schemas {
  [ className: string ]: Parse.Schema
}
export interface CloudConfig {
  reset: boolean
  sync: boolean
  module: Promise<any> | any
}
export interface Setup {
  onStart(cloud: Cloud): void
}

export type Resolvers<T, U = Parse.Attributes> = {
  [ className: string ]: {
    [ resolverName: string ]: (object: T, args: any, user: Parse.User<U>, info: any) => Promise<any> | any
  }
};

class Logger {
  color(text: string, color: number) {
    return `\u{1b}[${color}m${text}\u{1b}[0m`;
  }
  log(level: "debug" | "info" | "warn" | "error", ...args): void {
    let colored;
    switch (level) {
      case "debug":
        colored = this.color(`[${level}]`, 34);
        break;
      case "info":
        colored = this.color(`[${level}]`, 32);
        break;
      case "warn":
        colored = this.color(`[warn]`, 33);
        break;
      case "error":
        colored = this.color(`[${level}]`, 31);
        break;
    }
    console[ level ](colored, ...args);
  }
  info(...args) {
    return this.log("info", ...args);
  }
  error(...args) {
    return this.log("error", ...args);
  }
  debug(...args) {
    return this.log("debug", ...args);
  }
  warn(...args) {
    return this.log("warn", ...args);
  }
}

class CloudSchema extends Parse.Schema {
  addField<T extends Parse.Schema.TYPE = any>(name: string, type?: T, options?: Parse.Schema.FieldOptions): this {
    super.addField(name, type, options);
    this[ "_fields" ][ name ] = {
      type,
      ...options
    };
    return this;
  }
}

export function isPrimitive(obj) {
  return (obj !== Object(obj));
}
export function deepEqual(obj1, obj2) {

  if (obj1 === obj2) // it's just the same object. No need to compare.
  {
    return true;
  }

  if (isPrimitive(obj1) && isPrimitive(obj2)) // compare primitives
  {
    return obj1 === obj2;
  }

  if (typeof obj1 !== "object" || typeof obj2 !== "object") {
    return false;
  }

  if (Object.keys(obj1).length !== Object.keys(obj2).length) {
    return false;
  }

  // compare objects with same number of keys
  for (let key in obj1) {
    if (!(key in obj2)) {
      return false;
    } //other object doesn't have this prop
    if (!deepEqual(obj1[ key ], obj2[ key ])) {
      return false;
    }
  }

  return true;
}

export const cloud = new Cloud();
