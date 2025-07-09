let JsonDiffPatch = require('jsondiffpatch'),
  semver = require('semver');

let historyPlugin = (options = {}) => {
  let pluginOptions = {
    mongoose: false, // A mongoose instance
    modelName: '__histories', // Name of the collection for the histories
    useMonthlyPartition: true,
    embeddedDocument: false, // Is this a sub document
    embeddedModelName: '', // Name of model if used with embedded document
    userCollection: 'users', // Collection to ref when you pass an user id
    userCollectionIdType: false, // Type for user collection ref id, defaults to ObjectId
    accountCollection: 'accounts', // Collection to ref when you pass an account id or the item has an account property
    accountCollectionIdType: false, // Type for account collection ref id, defaults to ObjectId
    userFieldName: 'user', // Name of the property for the user
    accountFieldName: 'account', // Name of the property of the account if any
    timestampFieldName: 'timestamp', // Name of the property of the timestamp
    methodFieldName: 'method', // Name of the property of the method
    collectionIdType: false, // Cast type for _id (support for other binary types like uuid)
    ignore: [], // List of fields to ignore when compare changes
    noDiffSave: false, // Save event even if there are no changes
    noDiffSaveOnMethods: [], // Save event even if there are no changes if method matches
    noEventSave: true, // If false save only when __history property is passed
    startingVersion: '0.0.0', // Default starting version

    // If true save only the _id of the populated fields
    // If false save the whole object of the populated fields
    // If false and a populated field property changes it triggers a new history
    // You need to populate the field after a change is made on the original document or it will not catch the differences
    ignorePopulatedFields: true,
  };

  Object.assign(pluginOptions, options);

  if (pluginOptions.mongoose === false) {
    throw new Error('You need to pass a mongoose instance');
  }

  let mongoose = pluginOptions.mongoose;

  const collectionIdType =
    options.collectionIdType || mongoose.Schema.Types.ObjectId;
  const userCollectionIdType =
    options.userCollectionIdType || mongoose.Schema.Types.ObjectId;
  const accountCollectionIdType =
    options.accountCollectionIdType || mongoose.Schema.Types.ObjectId;

  let Schema = new mongoose.Schema(
    {
      collectionName: String,
      collectionId: { type: collectionIdType },
      diff: {},
      event: String,
      reason: String,
      data: { type: mongoose.Schema.Types.Mixed },
      [pluginOptions.userFieldName]: {
        type: userCollectionIdType,
        ref: pluginOptions.userCollection,
      },
      [pluginOptions.accountFieldName]: {
        type: accountCollectionIdType,
        ref: pluginOptions.accountCollection,
      },
      version: { type: String, default: pluginOptions.startingVersion },
      [pluginOptions.timestampFieldName]: Date,
      [pluginOptions.methodFieldName]: String,
    },
    {
      collection: pluginOptions.modelName,
    }
  );

  Schema.set('minimize', false);
  Schema.set('versionKey', false);
  Schema.set('strict', true);

  Schema.pre('save', function (next) {
    this[pluginOptions.timestampFieldName] = new Date();
    next();
  });
  /* 
  let Model = mongoose.model(pluginOptions.modelName, Schema);

  let getModelName = (defaultName) => {
    return pluginOptions.embeddedDocument ? pluginOptions.embeddedModelName : defaultName;
  }; */
  const getModelName = (doc) =>
    pluginOptions.embeddedDocument ? pluginOptions.embeddedModelName : doc;
  let jdf = JsonDiffPatch.create({
    objectHash: function (obj, index) {
      if (obj !== undefined) {
        return (
          (obj._id && obj._id.toString()) ||
          obj.id ||
          obj.key ||
          '$$index:' + index
        );
      }

      return '$$index:' + index;
    },
    arrays: {
      detectMove: true,
    },
  });
  const getPartitionedModel = (yearMonthSuffix) => {
    if (!pluginOptions.useMonthlyPartition)
      return mongoose.model(pluginOptions.modelName);
    const collectionName = pluginOptions.modelName + yearMonthSuffix;
    return (
      mongoose.models[collectionName] ||
      mongoose.model(collectionName, Schema, collectionName)
    );
  };

  const formatDate = (isoDate) => {
    const day = String(isoDate.getDate()).padStart(2, '0');
    const month = String(isoDate.getMonth() + 1).padStart(2, '0');
    const year = String(isoDate.getFullYear()).slice(-2);
    const hours = String(isoDate.getHours()).padStart(2, '0');
    const minutes = String(isoDate.getMinutes()).padStart(2, '0');

    return `${day}/${month}/${year} ${hours}:${minutes}`;
  };
  let query = async (method = 'find', options = {}, date = null) => {
    const baseName = pluginOptions.modelName; // "__histories"
    if (options.select !== undefined) {
      Object.assign(options.select, {
        _id: 0,
        collectionId: 0,
        collectionName: 0,
      });
    }
    if (date) {
      const yearMonthSuffix = `_${date.getFullYear()}_${(date.getMonth() + 1)
        .toString()
        .padStart(2, '0')}`;
      const Model = getPartitionedModel(yearMonthSuffix);
      const query = Model[method](options.find || {});
      options.select && query.select(options.select);
      options.sort && query.sort(options.sort);
      options.populate && query.populate(options.populate);
      options.limit && query.limit(options.limit);
      return query.lean();
    }

    // Caso date não seja fornecida → busca em todas coleções particionadas
    const collections = await mongoose.connection.db
      .listCollections()
      .toArray();
    const historyCollections = collections
      .map((col) => col.name)
      .filter((name) => name.startsWith(baseName + '_'));

    const allResults = [];

    for (const name of historyCollections) {
      try {
        const Model =
          mongoose.models[name] || mongoose.model(name, Schema, name);
        const query = Model[method](options.find || {});
        options.select && query.select(options.select);
        options.sort && query.sort(options.sort);
        options.populate && query.populate(options.populate);
        options.limit && query.limit(options.limit);
        const docs = await query.lean();
        allResults.push(...docs);
      } catch (err) {
        console.warn(`Erro ao consultar coleção ${name}:`, err.message);
      }
    }

    // Opcional: ordena os resultados globalmente, se houver `sort` e sort form "timestamp"
    if (options.sort && options.sort === 'timestamp') {
      const isDesc = options.sort.startsWith('-');
      allResults.sort((a, b) => {
        const av = new Date(a['timestamp']);
        const bv = new Date(b['timestamp']);
        return isDesc ? bv - av : av - bv;
      });
    }

    // Aplica limit global (se necessário)
    if (options.limit) {
      return allResults.slice(0, options.limit);
    }

    return allResults;
  };

  let getPreviousVersion = async (document) => {
    // get the oldest version from the history collection
    let versions = await document.getVersions();
    return versions[versions.length - 1]
      ? versions[versions.length - 1].object
      : {};
  };

  let getPopulatedFields = (document) => {
    let populatedFields = [];
    // we only depopulate the first depth of fields
    for (let field in document) {
      if (document.populated(field)) {
        populatedFields.push(field);
      }
    }

    return populatedFields;
  };

  let depopulate = (document, populatedFields) => {
    // we only depopulate the first depth of fields
    for (let field of populatedFields) {
      document.depopulate(field);
    }
  };

  let repopulate = async (document, populatedFields) => {
    for (let field of populatedFields) {
      await document.populate(field).execPopulate();
    }
  };

  let cloneObjectByJson = (object) =>
    object ? JSON.parse(JSON.stringify(object)) : {};

  let cleanFields = (object) => {
    delete object.__history;
    delete object.__v;

    for (let i in pluginOptions.ignore) {
      delete object[pluginOptions.ignore[i]];
    }
    return object;
  };

  let getDiff = ({ prev, current, document, forceSave }) => {
    let diff = jdf.diff(prev, current);

    let saveWithoutDiff = false;
    if (document.__history && pluginOptions.noDiffSaveOnMethods.length) {
      let method = document.__history[pluginOptions.methodFieldName];
      if (pluginOptions.noDiffSaveOnMethods.includes(method)) {
        saveWithoutDiff = true;
        if (forceSave) {
          diff = prev;
        }
      }
    }

    return {
      diff,
      saveWithoutDiff,
    };
  };

  let saveHistory = async ({ document, diff }) => {
    const date = new Date(); // Data a partiar da qual será extraída o ano e mês para saber em qual coleção salvar o novo documento .
    const yearMonthSuffix = `_${date.getFullYear()}_${(date.getMonth() + 1)
      .toString()
      .padStart(2, '0')}`;
    const Model = getPartitionedModel(yearMonthSuffix);

    let lastHistory = await Model.findOne({
      collectionName: getModelName(document.constructor.modelName),
      collectionId: document._id,
    })
      .sort('-' + pluginOptions.timestampFieldName)
      .select({ version: 1 });

    let obj = {};
    obj.collectionName = getModelName(document.constructor.modelName);
    obj.collectionId = document._id;
    obj.diff = diff || {};

    if (document.__history) {
      (obj.collectionName = getModelName(document.constructor.modelName)),
        (obj.collectionId = document._id),
        diff,
        (obj.event = document.__history.event);
      obj[pluginOptions.userFieldName] =
        document.__history[pluginOptions.userFieldName];
      obj[pluginOptions.accountFieldName] =
        document[pluginOptions.accountFieldName] ||
        document.__history[pluginOptions.accountFieldName];
      obj.reason = document.__history.reason;
      obj.data = document.__history.data;
      obj[pluginOptions.methodFieldName] =
        document.__history[pluginOptions.methodFieldName];
    }

    let version;

    if (lastHistory) {
      let type =
        document.__history && document.__history.type
          ? document.__history.type
          : 'major';

      version = semver.inc(lastHistory.version, type);
    }

    obj.version = version || pluginOptions.startingVersion;
    for (let i in obj) {
      if (obj[i] === undefined) {
        delete obj[i];
      }
    }

    let history = new Model(obj);

    document.__history = undefined;
    await history.save();
  };

  return function (schema) {
    schema.add({
      __history: { type: mongoose.Schema.Types.Mixed },
    });

    let preSave = function (forceSave) {
      return async function (next) {
        let currentDocument = this;
        if (
          currentDocument.__history !== undefined ||
          pluginOptions.noEventSave
        ) {
          try {
            let previousVersion = await getPreviousVersion(currentDocument);
            let populatedFields = getPopulatedFields(currentDocument);

            if (pluginOptions.ignorePopulatedFields) {
              depopulate(currentDocument, populatedFields);
            }

            let currentObject = cleanFields(cloneObjectByJson(currentDocument));
            let previousObject = cleanFields(
              cloneObjectByJson(previousVersion)
            );

            if (pluginOptions.ignorePopulatedFields) {
              await repopulate(currentDocument, populatedFields);
            }

            let { diff, saveWithoutDiff } = getDiff({
              current: currentObject,
              prev: previousObject,
              document: currentDocument,
              forceSave,
            });

            if (diff || pluginOptions.noDiffSave || saveWithoutDiff) {
              await saveHistory({ document: currentDocument, diff });
            }

            return next();
          } catch (error) {
            return next(error);
          }
        }

        next();
      };
    };

    schema.pre('save', preSave(false));

    schema.pre('remove', preSave(true));

    // diff.find
    /**
     * Recupera as diferenças (diffs) de um documento a partir de opções fornecidas.
     *
     * @function
     * @name getDiffs
     * @param {Object} [options={}] - Opções de busca para os diffs.
     * @param {Object} [options.find] - Filtros adicionais para a busca.
     * @param {string} [options.sort] - Campo de ordenação (padrão: timestamp decrescente).
     * @param {Date} [date] - Data a partiar da qual será extraída o ano e mês para saber em qual coleção será executada a busca.
     *
     * @returns {Promise<Array>} Retorna uma Promise que resolve para um array de diffs encontrados.
     *
     */
    schema.methods.getDiffs = function (options = {}, date) {
      options.find = options.find || {};
      Object.assign(options.find, {
        collectionName: getModelName(this.constructor.modelName),
        collectionId: this._id,
      });
      options.sort = options.sort || '-' + pluginOptions.timestampFieldName;

      return query('find', options, date);
    };

    schema.methods.getCompleteSnapshots = async function (
      versions = [],
      date,
      fieldsSelected,
      desc = true,
      getDiffId = false
    ) {
      const allDiffs = await this.getDiffs(
        {
          sort: `${pluginOptions.timestampFieldName}`,
        },
        date
      );

      const semverCompare = (a, b) => {
        if (semver.gt(a, b)) return 1;
        if (semver.lt(a, b)) return -1;
        return 0;
      };

      const jdf = JsonDiffPatch.create({
        objectHash: function (obj, index) {
          return (obj && (obj._id || obj.id || obj.key)) || `$$index:${index}`;
        },
        arrays: {
          detectMove: true,
        },
      });

      const result = [];
      let current = {};
      let i = 0;
      const pushedVersions = new Set();

      const sortedTargets = [...versions].sort((a, b) => semverCompare(a, b));

      for (const diffEntry of allDiffs) {
        const nextTarget = sortedTargets[i];
        const cmp = semverCompare(diffEntry.version, nextTarget);

        current = jdf.patch(current, diffEntry.diff);

        if (cmp === 0 /* && !pushedVersions.has(diffEntry.version) */) {
          const filtered = {};
          fieldsSelected.forEach((field) => {
            filtered[field] = current[field];
          });
          filtered['datahora'] = formatDate(diffEntry.timestamp);

          const isDuplicate =
            result.length > 0 &&
            JSON.stringify(result[0]) === JSON.stringify(filtered);

          if (/* true */ !isDuplicate) {
            if (desc) {
              result.unshift(JSON.parse(JSON.stringify(filtered)));
            } else {
              result.push(JSON.parse(JSON.stringify(filtered)));
            }
            pushedVersions.add(diffEntry.version);
            i++;
          } else {
            // se for duplicado, ainda precisamos avançar i (senão trava no mesmo diff)
            pushedVersions.add(diffEntry.version);
            i++;
          }
        }

        if (i >= sortedTargets.length) break;
      }

      return result;
    };

    // diff.get
    schema.methods.getDiff = function (version, options = {}, date) {
      const monthSuffix = `_${date.getFullYear()}_${(date.getMonth() + 1)
        .toString()
        .padStart(2, '0')}`;

      options.find = options.find || {};
      Object.assign(options.find, {
        collectionName: getModelName(this.constructor.modelName) + monthSuffix,
        collectionId: this._id,
        version: version,
      });

      options.sort = options.sort || '-' + pluginOptions.timestampFieldName;

      return query('findOne', options, date);
    };

    // versions.get
    schema.methods.getVersion = async function (
      version2get,
      includeObject = true
    ) {
      let histories = await this.getDiffs({
        sort: pluginOptions.timestampFieldName,
      });

      let lastVersion = histories[histories.length - 1],
        firstVersion = histories[0],
        history,
        version = {};

      if (semver.gt(version2get, lastVersion.version)) {
        version2get = lastVersion.version;
      }

      if (semver.lt(version2get, firstVersion.version)) {
        version2get = firstVersion.version;
      }

      histories.map((item) => {
        if (item.version === version2get) {
          history = item;
        }
      });

      if (!includeObject) {
        return history;
      }

      histories.map((item) => {
        if (
          semver.lt(item.version, version2get) ||
          item.version === version2get
        ) {
          version = jdf.patch(version, item.diff);
        }
      });

      delete history.diff;
      history.object = version;

      return history;
    };

    // versions.compare
    schema.methods.compareVersions = async function (
      versionLeft,
      versionRight
    ) {
      let versionLeftDocument = await this.getVersion(versionLeft);
      let versionRightDocument = await this.getVersion(versionRight);

      return {
        diff: jdf.diff(versionLeftDocument.object, versionRightDocument.object),
        left: versionLeftDocument.object,
        right: versionRightDocument.object,
      };
    };

    // versions.find
    schema.methods.getVersions = async function (
      options = {},
      includeObject = true
    ) {
      options.sort = options.sort || pluginOptions.timestampFieldName;

      let histories = await this.getDiffs(options);

      if (!includeObject) {
        return histories;
      }

      let version = {};
      for (let i = 0; i < histories.length; i++) {
        version = jdf.patch(version, histories[i].diff);
        histories[i].object = jdf.clone(version);
        delete histories[i].diff;
      }

      return histories;
    };
  };
};

module.exports = historyPlugin;
